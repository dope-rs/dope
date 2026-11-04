//! Bounded, owned file operations retained through native completion.

use std::{cell, marker, num::NonZero, pin};

use dope_core::{
    driver::{
        self,
        result::{SubmitCause, SubmitError},
        retained::flight,
        route::{self, Identity, indices, lease, table},
        schedule::{self, ready::wake},
        settings::{self, claim, demands, errors},
    },
    io::{fs, transfer, transfer::Length as _},
};
use dope_runtime::executor;
use o3::{
    alloc,
    buffer::{bytes::bounded, resident, storage},
    depot::queue::indexed::shared,
    mem::{Rejected, quota},
};

use crate::app::dispatch;

mod raw;

const FREE: usize = 0;
const WORK: usize = 1;

/// Initialized, exclusive storage carrying its original resident-byte charge.
pub type Buffer<'d> = bounded::Value<storage::Owned<resident::Lease<'d>>>;

/// A nonempty read whose buffer extent was admitted exactly once.
pub struct Read<'d> {
    file: fs::File,
    output: Buffer<'d>,
    offset: fs::Offset,
    length: transfer::NonZero,
}

/// One request owns every descriptor, path, option, and output it submits.
pub enum Request<'d> {
    Open(fs::OpenRequest),
    Read(Read<'d>),
}

/// A terminal native operation returns its original request owners unchanged.
pub enum Completion<'d> {
    Open {
        request: fs::OpenRequest,
        result: Result<fs::File, Error>,
    },
    Read {
        request: Read<'d>,
        result: Result<transfer::Len, Error>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    System(dope_core::Errno),
    Submission(SubmitError),
    Amount {
        observed: transfer::Len,
        requested: transfer::NonZero,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Full,
    Closed,
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

    pub fn try_port<'d>(&self) -> Result<Port<'d>, CreateError> {
        Port::try_new(self.capacity)
    }
}

impl<ID: Identity> settings::Resource for Layout<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(
            claim::Claim::new(settings::Routes::singleton::<ID>()).files(demands::Files::only_out(
                demands::Outbound::new(self.capacity.count()),
            )),
        )
    }
}

impl<'d> Read<'d> {
    pub fn try_new(
        file: fs::File,
        output: Buffer<'d>,
        offset: fs::Offset,
    ) -> Result<Self, Rejected<(fs::File, Buffer<'d>, fs::Offset), storage::Zero>> {
        let Some(length) = output.len().nonzero() else {
            return Err(Rejected::new((file, output, offset), storage::Zero));
        };
        Ok(Self {
            file,
            output,
            offset,
            length,
        })
    }

    /// Reuses the same initialized allocation without filling or copying it.
    pub fn at(mut self, offset: fs::Offset) -> Self {
        self.offset = offset;
        self
    }

    pub const fn length(&self) -> transfer::NonZero {
        self.length
    }

    /// Only the successful completion's prefix contains newly read bytes.
    pub fn into_parts(self) -> (fs::File, Buffer<'d>, fs::Offset) {
        (self.file, self.output, self.offset)
    }
}

type Slot<'a, 'd> = shared::Slot<'a, (), raw::Entry<'d>, 2>;

/// A worker-local request/reply port. Construct once, then install one Reader.
///
/// Slots bound queued, native, and uncollected operations together. Buffer
/// residency remains charged by each supplied owner, including after completion.
pub struct Port<'d> {
    slots: shared::Fifo<(), raw::Entry<'d>, 2>,
    capacity: table::Capacity,
    bound: cell::Cell<bool>,
    accepting: cell::Cell<bool>,
    pending: cell::Cell<usize>,
}

/// The installed owner retains the port and native flight group until Finish.
pub struct Reader<'p, 'd, ID: Identity> {
    port: &'p Port<'d>,
    _route: lease::Owner<'d, ID>,
    flights: flight::Slots<'d, ID>,
    closing: Option<indices::SlotIndex>,
}

// No field is structurally pinned: the borrowed port retains its fixed heap
// slots, and the route and flight handles do not retain their own addresses.
impl<ID: Identity> Unpin for Reader<'_, '_, ID> {}

/// An affine reply registration; dropping it requests cancellation, not reclamation.
#[must_use = "poll the operation or drop its ticket to cancel it"]
pub struct Ticket<'p, 'd> {
    slot: Option<Slot<'p, 'd>>,
}

#[must_use = "a pending operation retains its reply registration"]
pub enum Poll<'p, 'd> {
    Pending(Ticket<'p, 'd>),
    Ready(Completion<'d>),
}

enum Outcome {
    Open(Result<fs::File, Error>),
    Read(Result<transfer::Len, Error>),
}

enum Active<'d> {
    Open(flight::Flight<'d, route::kind::Open>),
    Read(flight::Flight<'d, route::kind::Read>),
}

enum Phase<'d> {
    Queued,
    Active { flight: Active<'d>, stopping: bool },
    Done(Outcome),
}

impl<'d> Port<'d> {
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
        request: Request<'d>,
    ) -> Result<Ticket<'_, 'd>, Rejected<Request<'d>, Admission>> {
        if !self.accepting.get() {
            return Err(Rejected::new(request, Admission::Closed));
        }
        let Some((slot, ())) = self.slots.next::<FREE>() else {
            return Err(Rejected::new(request, Admission::Full));
        };
        let entry = slot.state();
        entry.insert(request);
        entry.detached.set(false);
        entry.phase.set(Some(Phase::Queued));
        self.pending.set(self.pending.get() + 1);
        slot.push::<WORK>(());
        Ok(Ticket { slot: Some(slot) })
    }

    fn settle(&self, slot: &Slot<'_, 'd>, outcome: Outcome) {
        slot.clear::<WORK>();
        self.pending.set(self.pending.get() - 1);
        let entry = slot.state();
        if entry.detached.get() {
            let request = entry.take();
            slot.push::<FREE>(());
            drop((request, outcome));
        } else {
            entry.phase.set(Some(Phase::Done(outcome)));
            if let Some(waiter) = entry.waiter.take() {
                waiter.wake();
            }
        }
    }

    fn fail(&self, slot: &Slot<'_, 'd>, cause: Error) {
        let outcome = slot.state().with_request(|request| match request {
            Request::Open(_) => Outcome::Open(Err(cause)),
            Request::Read(_) => Outcome::Read(Err(cause)),
        });
        self.settle(slot, outcome);
    }
}

impl<'p, 'd> Ticket<'p, 'd> {
    pub fn poll(mut self, waiter: wake::Waker<'d>) -> Poll<'p, 'd> {
        let Some(slot) = self.slot.take() else {
            unreachable!("ticket")
        };
        let entry = slot.state();
        match entry.phase.take() {
            Some(Phase::Done(outcome)) => {
                let request = entry.take();
                slot.push::<FREE>(());
                Poll::Ready(match (request, outcome) {
                    (Request::Open(request), Outcome::Open(result)) => {
                        Completion::Open { request, result }
                    }
                    (Request::Read(request), Outcome::Read(result)) => {
                        Completion::Read { request, result }
                    }
                    _ => unreachable!("operation"),
                })
            }
            phase @ Some(Phase::Queued | Phase::Active { .. }) => {
                entry.waiter.set(Some(waiter));
                entry.phase.set(phase);
                Poll::Pending(Self { slot: Some(slot) })
            }
            None => unreachable!("ticket"),
        }
    }
}

impl Drop for Ticket<'_, '_> {
    fn drop(&mut self) {
        let Some(slot) = self.slot.take() else { return };
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

impl<'p, 'd, ID: Identity> Reader<'p, 'd, ID> {
    pub fn try_new(
        port: &'p Port<'d>,
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
        })
    }

    fn close(&mut self) {
        if self.port.accepting.replace(false) {
            self.closing = Some(indices::SlotIndex::ZERO);
        }
    }

    fn advance<'root>(
        &mut self,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, dispatch::raw::Retained>,
    ) {
        loop {
            let next = turn.app().run(|| {
                if let Some((slot, ())) = self.port.slots.next::<WORK>() {
                    return Some(slot);
                }
                let index = self.closing?;
                self.closing = self.port.capacity.next(index);
                self.port.slots.get(index.index().into_usize())
            });
            let slot = match next {
                quota::Admission::Item(slot) => slot,
                quota::Admission::Empty | quota::Admission::Exhausted => return,
            };
            if !self.process(&slot, driver) {
                return;
            }
        }
    }

    fn process<'root>(
        &self,
        slot: &Slot<'_, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, dispatch::raw::Retained>,
    ) -> bool {
        let entry = slot.state();
        let stop = entry.detached.get() || !self.port.accepting.get();
        match entry.phase.take() {
            Some(Phase::Queued) => {
                if stop {
                    self.port.fail(slot, Error::Shutdown);
                    return true;
                }
                match raw::submit(entry, &self.flights, driver) {
                    Ok(flight) => entry.phase.set(Some(Phase::Active {
                        flight,
                        stopping: false,
                    })),
                    Err(cause) => match cause.cause() {
                        SubmitCause::Full => {
                            entry.phase.set(Some(Phase::Queued));
                            slot.push::<WORK>(());
                            return false;
                        }
                        SubmitCause::IdentityExhausted => {
                            self.port.fail(slot, Error::Submission(cause))
                        }
                    },
                }
            }
            Some(Phase::Active {
                mut flight,
                mut stopping,
            }) => {
                let mut retry = false;
                let mut rejected = None;
                if stop && !stopping {
                    match flight.stop(driver.driver()) {
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
                    driver.driver().rejected(cause);
                }
                if retry {
                    slot.push::<WORK>(());
                    return false;
                }
            }
            phase @ (Some(Phase::Done(_)) | None) => entry.phase.set(phase),
        }
        true
    }
}

impl<ID: Identity> Drop for Reader<'_, '_, ID> {
    fn drop(&mut self) {
        self.port.accepting.set(false);
    }
}

impl<'d> Active<'d> {
    fn stop(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<flight::cancel::Outcome, flight::cancel::Failure> {
        match self {
            Self::Open(flight) => flight.try_stop(driver),
            Self::Read(flight) => flight.try_stop(driver),
        }
    }
}

impl<'d, ID: Identity> dispatch::raw::Install<'d> for Reader<'_, 'd, ID> {
    fn install(self: pin::Pin<&mut Self>) {
        self.port.accepting.set(true);
    }
}

impl<'d, ID: Identity> executor::raw::Finishes<'d, Self> for Reader<'_, 'd, ID> {
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let this = owner.as_mut().get_mut();
        this.port.accepting.set(false);
        this.closing = None;
        // Finish carries completed native quiescence, including forced shutdown.
        // This final O(capacity) cleanup is outside the per-turn work allowance.
        for slot in this.port.slots.iter() {
            match slot.state().phase.take() {
                Some(Phase::Queued | Phase::Active { .. }) => {
                    this.port.fail(&slot, Error::Shutdown)
                }
                phase => slot.state().phase.set(phase),
            }
        }
        executor::raw::Finish::new(owner, install, proof)
    }
}
