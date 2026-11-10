//! Bounded, owned file operations retained through native completion.

use std::{cell, marker, num::NonZero, pin};

use dope_core::{
    driver::{
        self,
        result::{SubmitCause, SubmitError},
        retained::flight,
        route::{self, Identity, indices, lease, table},
        schedule::{self, ready::wake},
        settings::{self, claim, errors},
    },
    io::{fs, transfer},
};
use dope_runtime::executor;
use o3::{
    alloc,
    buffer::{bytes::bounded, resident, storage},
    depot::queue::indexed::shared,
    mem::{self, Rejected, Return, lease::Open, quota},
};

use crate::{
    app::dispatch,
    file::appender::{self, Preparation, Prepare},
};

mod raw;

const FREE: usize = 0;
const WORK: usize = 1;

/// Exclusive storage carrying its original resident-byte charge.
pub type Buffer<'d> = storage::Owned<resident::Budget<'d>>;

#[derive(Debug, Default)]
pub struct Release<'d>(marker::PhantomData<Buffer<'d>>);

impl<'d> Return for Release<'d> {
    type Item = Buffer<'d>;

    #[inline]
    fn return_item(self, item: Self::Item) {
        drop(item);
    }
}

/// A read whose nonempty buffer capacity was admitted exactly once.
pub struct Read<R: Return> {
    file: fs::File,
    output: mem::Lease<R, Open>,
    offset: fs::Offset,
    length: transfer::NonZero,
}

pub type Parts<R> = (fs::File, mem::Lease<R, Open>, fs::Offset);

pub struct Metadata {
    file: fs::File,
    state: MetadataState,
}

enum MetadataState {
    Idle,
    Scratch(Box<raw::Scratch>),
    Done(Result<fs::metadata::Value, MetadataError>),
}

impl Metadata {
    pub fn new(file: fs::File) -> Self {
        Self {
            file,
            state: MetadataState::Idle,
        }
    }

    #[inline]
    fn release(&mut self, free: &mut Option<Box<raw::Scratch>>) {
        if let MetadataState::Scratch(scratch) =
            std::mem::replace(&mut self.state, MetadataState::Idle)
        {
            raw::Scratch::release(free, scratch);
        }
    }
}

/// One request owns every descriptor, path, option, and output it submits.
pub enum Request<R: Return> {
    Open(fs::OpenRequest),
    Rename {
        directory: fs::Dir,
        old: fs::path::Owned,
        new: fs::path::Owned,
    },
    Read(Read<R>),
    Metadata(Metadata),
    Prepare(Prepare),
}

/// A terminal native operation returns its original request owners unchanged.
pub enum Completion<R: Return> {
    Open {
        request: fs::OpenRequest,
        result: Result<fs::File, Error>,
    },
    Rename {
        directory: fs::Dir,
        old: fs::path::Owned,
        new: fs::path::Owned,
        result: Result<(), Error>,
    },
    Read {
        request: Read<R>,
        result: Result<transfer::Len, Error>,
    },
    Metadata {
        file: fs::File,
        result: Result<fs::metadata::Value, MetadataError>,
    },
    Prepare(Result<appender::Prepared, appender::OpenError>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    System(dope_core::Errno),
    Submission(SubmitError),
    Allocation(alloc::Exhausted),
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataError {
    Operation(Error),
    Value(fs::metadata::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Full,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeError {
    Empty,
    Overflow(bounded::Overflow),
}

#[derive(Debug)]
pub enum CreateError {
    Capacity(table::Invalid),
    Allocation(alloc::Error),
    Route(lease::Error),
    Bound,
}

/// One route and its positive request capacity, shared by planning and storage.
pub struct Layout<ID: Identity> {
    capacity: table::Capacity,
    route: marker::PhantomData<fn() -> ID>,
}

impl<ID: Identity> Layout<ID> {
    pub fn try_new(count: NonZero<u32>) -> Result<Self, CreateError> {
        let capacity = table::Capacity::try_nonzero(count).map_err(CreateError::Capacity)?;
        Ok(Self {
            capacity: capacity.into(),
            route: marker::PhantomData,
        })
    }

    pub fn try_port<'d, R: Return<Item = Buffer<'d>>>(&self) -> Result<Port<'d, R>, CreateError> {
        Port::try_new(self.capacity)
    }
}

impl<ID: Identity> settings::Resource for Layout<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(claim::Claim::new(settings::Routes::singleton::<ID>()))
    }
}

impl<'d, R: Return<Item = Buffer<'d>>> Read<R> {
    pub fn try_new(
        file: fs::File,
        output: mem::Lease<R, Open>,
        offset: fs::Offset,
    ) -> Result<Self, Rejected<Parts<R>, SizeError>> {
        let length = transfer::Len::try_new(output.item().capacity())
            .map_err(SizeError::Overflow)
            .and_then(|length| length.nonzero().ok_or(SizeError::Empty));
        let length = match length {
            Ok(length) => length,
            Err(cause) => return Err(Rejected::new((file, output, offset), cause)),
        };
        Ok(Self {
            file,
            output,
            offset,
            length,
        })
    }

    /// Reuses the admitted allocation without filling or copying it.
    pub fn at(mut self, offset: fs::Offset) -> Self {
        self.offset = offset;
        self
    }

    pub const fn offset(&self) -> fs::Offset {
        self.offset
    }

    /// A completed request exposes only its successfully read prefix.
    pub fn into_parts(self) -> Parts<R> {
        (self.file, self.output, self.offset)
    }
}

type Slot<'a, 'd, R> = shared::Slot<'a, (), raw::Entry<'d, R>, 2>;

/// A worker-local request/reply port. Construct once, then install one Reader.
///
/// Slots bound queued, native, and uncollected operations together. Buffer
/// residency remains charged by each supplied owner, including after completion.
pub struct Port<'d, R: Return = Release<'d>> {
    slots: shared::Fifo<(), raw::Entry<'d, R>, 2>,
    capacity: table::Capacity,
    bound: cell::Cell<bool>,
    accepting: cell::Cell<bool>,
    pending: cell::Cell<usize>,
}

/// The installed owner retains the port and native flight group until Finish.
pub struct Reader<'p, 'd, ID: Identity, R: Return = Release<'d>> {
    port: &'p Port<'d, R>,
    _route: lease::Owner<'d, ID>,
    flights: flight::Slots<'d, ID>,
    closing: Option<indices::SlotIndex>,
    free: Option<Box<raw::Scratch>>,
}

// No field is structurally pinned: the borrowed port retains its fixed heap
// slots, and the route and flight handles do not retain their own addresses.
impl<ID: Identity, R: Return> Unpin for Reader<'_, '_, ID, R> {}

/// An affine reply registration; dropping it requests cancellation, not reclamation.
#[must_use = "poll the operation or drop its ticket to cancel it"]
pub struct Ticket<'p, 'd, R: Return = Release<'d>> {
    slot: Slot<'p, 'd, R>,
}

#[must_use = "a pending operation retains its reply registration"]
pub enum Poll<'p, 'd, R: Return = Release<'d>> {
    Pending(Ticket<'p, 'd, R>),
    Ready(Completion<R>),
}

enum Outcome {
    Open(Result<fs::File, Error>),
    Rename(Result<(), Error>),
    Read(Result<transfer::Len, Error>),
    Metadata,
    Prepare,
}

enum Active<'d> {
    Open(flight::Flight<'d, route::kind::Open>),
    Read(flight::Flight<'d, route::kind::Read>),
    Sync(flight::Flight<'d, route::kind::Sync>),
    Tail(flight::Flight<'d, route::kind::Read>),
}

enum Phase<'d> {
    Queued,
    Active { flight: Active<'d>, stopping: bool },
    Done(Outcome),
}

impl<'d, R: Return<Item = Buffer<'d>>> Port<'d, R> {
    fn try_new(capacity: table::Capacity) -> Result<Self, CreateError> {
        let mut indices = capacity.into_iter();
        let slots = shared::Fifo::try_with(capacity.count().get(), || {
            let Some(index) = indices.next() else {
                unreachable!("capacity")
            };
            raw::Entry::new(index)
        })
        .map_err(CreateError::Allocation)?;
        for slot in slots.iter() {
            slot.push::<FREE>(());
        }
        Ok(Self {
            slots,
            capacity,
            bound: cell::Cell::new(false),
            accepting: cell::Cell::new(false),
            pending: cell::Cell::new(0),
        })
    }

    /// Returns the unchanged request immediately on Full or Closed; no capacity
    /// waiter is registered. The caller retains its existing task admission policy.
    pub fn try_request(
        &self,
        mut request: Request<R>,
    ) -> Result<Ticket<'_, 'd, R>, Rejected<Request<R>, Admission>> {
        if !self.accepting.get() {
            return Err(Rejected::new(request, Admission::Closed));
        }
        let Some((slot, ())) = self.slots.next::<FREE>() else {
            return Err(Rejected::new(request, Admission::Full));
        };
        if let Request::Read(read) = &mut request {
            read.prepare();
        }
        let entry = slot.state();
        entry.insert(request);
        entry.detached.set(false);
        entry.phase.set(Some(Phase::Queued));
        self.pending.set(self.pending.get() + 1);
        slot.push::<WORK>(());
        Ok(Ticket { slot })
    }

    fn settle(&self, slot: &Slot<'_, 'd, R>, outcome: Outcome) -> bool {
        slot.clear::<WORK>();
        self.pending.set(self.pending.get() - 1);
        let entry = slot.state();
        if entry.detached.get() {
            let request = entry.take();
            slot.push::<FREE>(());
            drop((request, outcome));
            false
        } else {
            entry.phase.set(Some(Phase::Done(outcome)));
            if let Some(waiter) = entry.waiter.take() {
                waiter.wake();
                false
            } else {
                true
            }
        }
    }
}

impl<'p, 'd, R: Return> Ticket<'p, 'd, R> {
    pub fn poll(self, waiter: Option<wake::Waker<'d>>) -> Poll<'p, 'd, R> {
        let slot = &self.slot;
        let entry = slot.state();
        match entry.phase.take() {
            Some(Phase::Done(outcome)) => {
                let request = entry.take();
                slot.push::<FREE>(());
                std::mem::forget(self);
                Poll::Ready(match (request, outcome) {
                    (Request::Open(request), Outcome::Open(result)) => {
                        Completion::Open { request, result }
                    }
                    (
                        Request::Rename {
                            directory,
                            old,
                            new,
                        },
                        Outcome::Rename(result),
                    ) => Completion::Rename {
                        directory,
                        old,
                        new,
                        result,
                    },
                    (Request::Read(request), Outcome::Read(result)) => {
                        Completion::Read { request, result }
                    }
                    (
                        Request::Metadata(Metadata {
                            file,
                            state: MetadataState::Done(result),
                        }),
                        Outcome::Metadata,
                    ) => Completion::Metadata { file, result },
                    (Request::Prepare(Prepare(Preparation::Done(result))), Outcome::Prepare) => {
                        Completion::Prepare(result)
                    }
                    _ => unreachable!("operation"),
                })
            }
            phase @ Some(Phase::Queued | Phase::Active { .. }) => {
                entry.waiter.set(waiter);
                entry.phase.set(phase);
                Poll::Pending(self)
            }
            None => unreachable!("ticket"),
        }
    }
}

impl<R: Return> Drop for Ticket<'_, '_, R> {
    fn drop(&mut self) {
        let slot = &self.slot;
        let entry = slot.state();
        entry.waiter.take();
        entry.detached.set(true);
        match entry.phase.take() {
            Some(Phase::Done(outcome)) => {
                let request = entry.take();
                slot.push::<FREE>(());
                drop((request, outcome));
            }
            phase @ Some(Phase::Queued | Phase::Active { .. }) => {
                entry.phase.set(phase);
                slot.push::<WORK>(());
            }
            None => unreachable!("ticket"),
        }
    }
}

impl<'p, 'd, ID: Identity, R: Return<Item = Buffer<'d>>> Reader<'p, 'd, ID, R> {
    pub fn try_new(
        port: &'p Port<'d, R>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Self, CreateError> {
        if port.bound.get() {
            return Err(CreateError::Bound);
        }
        let mut route = lease::Owner::try_reserve(driver).map_err(CreateError::Route)?;
        let flights = route
            .driver()
            .storage
            .reborrow()
            .try_flights(flight::Need::<ID>::new(flight::Capacity::table(
                port.capacity,
            )))
            .map_err(CreateError::Allocation)?
            .slots();
        port.bound.set(true);
        Ok(Self {
            port,
            _route: route.commit(),
            flights,
            closing: None,
            free: None,
        })
    }

    fn fail(&mut self, slot: &Slot<'_, 'd, R>, cause: Error) -> bool {
        let outcome = slot.state().with_request(|request| match request {
            Request::Open(_) => Outcome::Open(Err(cause)),
            Request::Rename { .. } => Outcome::Rename(Err(cause)),
            Request::Read(_) => Outcome::Read(Err(cause)),
            Request::Metadata(request) => {
                request.release(&mut self.free);
                request.state = MetadataState::Done(Err(MetadataError::Operation(cause)));
                Outcome::Metadata
            }
            Request::Prepare(Prepare(preparation)) => {
                if let Preparation::Metadata { request, .. } = preparation {
                    request.release(&mut self.free);
                }
                *preparation = Preparation::Done(Err(appender::OpenError::Request(cause)));
                Outcome::Prepare
            }
        });
        self.port.settle(slot, outcome)
    }

    fn prepare(&self, slot: &Slot<'_, 'd, R>, preparation: Preparation) {
        let done = matches!(preparation, Preparation::Done(_));
        let entry = slot.state();
        entry.insert(Request::Prepare(Prepare(preparation)));
        if done {
            self.port.settle(slot, Outcome::Prepare);
        } else {
            entry.phase.set(Some(Phase::Queued));
            slot.push::<WORK>(());
        }
    }

    fn close(&mut self) {
        if self.port.accepting.replace(false) {
            self.closing = Some(indices::SlotIndex::ZERO);
        }
    }

    fn advance(
        &mut self,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, dispatch::raw::Retained>,
    ) {
        let port = self.port;
        loop {
            let next = turn.app().run(|| {
                if let Some((slot, ())) = port.slots.next::<WORK>() {
                    return Some(slot);
                }
                let index = self.closing?;
                self.closing = port.capacity.next(index);
                port.slots.get(index.index().into_usize())
            });
            let slot = match next {
                quota::Admission::Item(slot) => slot,
                quota::Admission::Empty | quota::Admission::Exhausted => return,
            };
            let entry = slot.state();
            let stop = entry.detached.get() || !port.accepting.get();
            match entry.phase.take() {
                Some(Phase::Queued) => {
                    let submitted = if stop {
                        Err(Error::Shutdown)
                    } else {
                        raw::submit(entry, &self.flights, &mut self.free, driver)
                    };
                    match submitted {
                        Ok(flight) => entry.phase.set(Some(Phase::Active {
                            flight,
                            stopping: false,
                        })),
                        Err(Error::Submission(cause))
                            if matches!(cause.cause(), SubmitCause::Full) =>
                        {
                            entry.phase.set(Some(Phase::Queued));
                            slot.push::<WORK>(());
                            return;
                        }
                        Err(cause) => {
                            if self.fail(&slot, cause) {
                                turn.app().defer();
                            }
                        }
                    }
                }
                Some(Phase::Active {
                    mut flight,
                    mut stopping,
                }) => {
                    let mut retry = false;
                    let mut rejected = None;
                    if stop && !stopping {
                        match flight.stop(driver) {
                            Ok(flight::cancel::Outcome::Deferred) => retry = true,
                            Ok(
                                flight::cancel::Outcome::Submitted
                                | flight::cancel::Outcome::Queued
                                | flight::cancel::Outcome::Completed,
                            ) => stopping = true,
                            Err(cause) => {
                                stopping = true;
                                rejected = Some(cause);
                            }
                        }
                    }
                    entry.phase.set(Some(Phase::Active { flight, stopping }));
                    if let Some(cause) = rejected {
                        driver.rejected(cause);
                    }
                    if retry {
                        slot.push::<WORK>(());
                        return;
                    }
                }
                phase @ (Some(Phase::Done(_)) | None) => entry.phase.set(phase),
            }
        }
    }
}

impl<ID: Identity, R: Return> Drop for Reader<'_, '_, ID, R> {
    fn drop(&mut self) {
        self.port.accepting.set(false);
        while let Some(mut scratch) = self.free.take() {
            self.free = scratch.next.take();
        }
    }
}

impl<'d> Active<'d> {
    fn stop(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<flight::cancel::Outcome, flight::cancel::Failure> {
        match self {
            Self::Open(flight) => flight.try_stop(driver),
            Self::Read(flight) | Self::Tail(flight) => flight.try_stop(driver),
            Self::Sync(flight) => flight.try_stop(driver),
        }
    }
}

impl<'d, ID: Identity, R: Return> dispatch::raw::Install<'d> for Reader<'_, 'd, ID, R> {
    fn install(self: pin::Pin<&mut Self>) {
        self.port.accepting.set(true);
    }
}

impl<'d, ID: Identity, R: Return<Item = Buffer<'d>>> executor::raw::Finishes<'d, Self>
    for Reader<'_, 'd, ID, R>
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let this = owner.as_mut().get_mut();
        this.port.accepting.set(false);
        this.closing = None;
        // Finish carries completed native quiescence, including forced shutdown.
        // This final O(capacity) cleanup is outside the per-turn work allowance.
        let port = this.port;
        for slot in port.slots.iter() {
            match slot.state().phase.take() {
                Some(Phase::Queued | Phase::Active { .. }) => {
                    this.fail(&slot, Error::Shutdown);
                }
                phase => slot.state().phase.set(phase),
            }
        }
        executor::raw::Finish::new(owner, install, proof)
    }
}
