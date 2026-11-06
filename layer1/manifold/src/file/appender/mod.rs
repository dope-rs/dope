use std::{marker, mem::MaybeUninit, ops, os::fd, path, pin};

use dope_core::{
    driver::{
        self,
        result::{SubmitCause, SubmitError},
        retained::flight,
        route::{self, Identity, indices, kind, lease},
    },
    io::{
        self,
        event::Kind,
        fs::{self, open},
        transfer,
    },
};
use dope_runtime::executor;
pub use layout::Layout;
use o3::{
    buffer::write::{Staged, consume},
    depot::queue::ring::inline,
    mem::Rejected,
};
pub use record::Record;
use state::State;
pub use storage::Storage;

use crate::{
    app::{creation, dispatch, dispatch::event},
    file,
};

mod queue;
pub(in crate::file) use queue::{InFlight, Pending, Queue};
mod layout;
mod record;
pub(in crate::file) mod state;
mod storage;

#[derive(Debug)]
pub enum WriteError {
    Full(inline::Full),
    TooLarge { requested: usize, maximum: usize },
    Capacity(consume::Error),
    Failed(Failure),
    Closed,
}
#[derive(Clone, Copy, Debug)]
pub struct Capacity {
    pub current: usize,
    pub added: usize,
    pub maximum: usize,
}

#[derive(Debug)]
pub enum OpenError {
    Open(open::Error),
    Lock(file::LockError),
    Metadata(fs::metadata::Error),
    Read(dope_core::Errno),
    Tail { expected: u8, observed: Option<u8> },
    Request(file::read::Error),
}
#[derive(Debug)]
pub enum RotateError {
    Failed(Failure),
    Closed,
    Busy,
}

pub struct Prepare(pub(in crate::file) Preparation);

pub(in crate::file) enum Preparation {
    Opening {
        request: fs::OpenRequest,
        tail: Option<u8>,
    },
    Metadata {
        request: file::read::Metadata,
        tail: Option<u8>,
    },
    Tail {
        prepared: Prepared,
        position: fs::Offset,
        output: MaybeUninit<u8>,
        expected: u8,
    },
    Done(Result<Prepared, OpenError>),
}

pub struct Prepared {
    pub(in crate::file) file: file::Locked,
    pub(in crate::file) end: fs::Offset,
}

impl Prepared {
    pub const fn end(&self) -> fs::Offset {
        self.end
    }
}

impl Prepare {
    pub fn new(path: fs::path::Confined, tail: Option<u8>) -> Self {
        Self(Preparation::Opening {
            request: fs::OpenRequest::new(path, options(tail)),
            tail,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Failure {
    System(dope_core::Errno),
    Submission(SubmitError),
    WriteZero,
    Cancelled,
}
#[repr(transparent)]
pub struct Control<'step, 'p, 'd, ID: Identity, const N: usize, const B: usize>
where
    'd: 'step,
{
    pub(super) inner: dispatch::raw::Installed<'step, 'd, Appender<'p, 'd, ID, N, B>>,
}
#[pin_project::pin_project]
pub struct Appender<'p, 'd, ID: Identity, const N: usize, const B: usize> {
    file: file::Locked,
    _route: lease::Owner<'d, ID>,
    pub(super) flights: flight::Slots<'d, <ID as route::Materialize<kind::Write>>::With>,
    pub(super) queue: Queue<'p, 'd, N, B>,
}
impl<'p, 'd, ID: Identity, const N: usize, const B: usize> executor::raw::Finishes<'d, Self>
    for Appender<'p, 'd, ID, N, B>
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let this = owner.as_mut().get_mut();
        if proof.is_forced() {
            this.cancel();
        }
        executor::raw::Finish::new(owner, install, proof)
    }
}
impl<'p, 'd, ID: Identity, const N: usize, const B: usize> Appender<'p, 'd, ID, N, B> {
    /// Opens an exclusively locked regular file for appending.
    ///
    /// A required tail admits empty files or a matching last byte before reserving a route.
    /// External truncation cannot leave a stale write offset, but copy-and-truncate
    /// rotation can still lose concurrent writes or split a partially written record.
    pub fn open(
        path: impl AsRef<path::Path>,
        required_tail: Option<u8>,
        storage: &'p mut Storage<N, B>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(Self, fs::Offset), creation::Error> {
        let prepared =
            open_file(path.as_ref(), required_tail).map_err(creation::Error::Appender)?;
        Self::try_from_prepared(prepared, storage, driver)
    }

    pub fn try_from_prepared(
        prepared: Prepared,
        storage: &'p mut Storage<N, B>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(Self, fs::Offset), creation::Error> {
        let Prepared { file, end } = prepared;
        let mut held = lease::Owner::try_reserve(driver).map_err(creation::Error::Route)?;
        let flights = held
            .driver()
            .storage
            .reborrow()
            .try_flights(
                flight::Need::<<ID as route::Materialize<kind::Write>>::With>::new(
                    flight::Capacity::fixed::<1>(),
                ),
            )
            .map_err(creation::Error::Allocation)?
            .slots();
        let route = held.commit();
        storage.failure.set(None);
        storage.confirmed.set(0);
        Ok((
            Self {
                file,
                _route: route,
                flights,
                queue: Queue {
                    storage,
                    current: None,
                    in_flight: None,
                    closed: false,
                },
            },
            end,
        ))
    }
    pub fn record<'a>(
        self: pin::Pin<&'a mut Self>,
        limit: usize,
    ) -> Result<Record<'a, 'd>, WriteError> {
        self.get_mut().begin(limit)
    }
    fn begin(&mut self, limit: usize) -> Result<Record<'_, 'd>, WriteError> {
        if let Some(failure) = self.queue.storage.failure() {
            return Err(WriteError::Failed(failure));
        }
        if self.queue.closed {
            return Err(WriteError::Closed);
        }
        if limit > B {
            return Err(WriteError::TooLarge {
                requested: limit,
                maximum: B,
            });
        }
        loop {
            let block = match self.queue.current.take() {
                Some(block) => block,
                None => self
                    .queue
                    .storage
                    .pool
                    .try_acquire()
                    .map_err(WriteError::Full)?,
            };
            if limit > B - block.get().len() {
                block.push_back(());
                continue;
            }
            let block = self.queue.current.insert(block).get_mut();
            let bytes = Staged::try_new(block, limit as u32).map_err(WriteError::Capacity)?;
            return Ok(Record {
                bytes,
                driver: marker::PhantomData,
            });
        }
    }
    pub(super) fn seal(&mut self) {
        let Some(block) = self.queue.current.take() else {
            return;
        };
        if block.get().is_empty() {
            block.release();
        } else {
            block.push_back(());
        }
    }
    pub(super) fn prepare(&mut self) -> Option<Pending<'p, N>> {
        if let Some(active) = self.queue.in_flight.take() {
            return match active {
                InFlight::Rejected { pending } if self.queue.storage.failure().is_none() => {
                    Some(pending)
                }
                active => {
                    self.queue.in_flight = Some(active);
                    None
                }
            };
        }
        self.seal();
        self.queue
            .storage
            .pool
            .pop_front()
            .map(|(block, ())| Pending::new(block))
    }
    pub(super) fn settle(
        &mut self,
        target: route::Erased<'d, kind::Write>,
    ) -> Option<Pending<'p, N>> {
        match self.queue.in_flight.take()? {
            InFlight::Submitted { pending, flight } if flight.matches(target) => {
                drop(flight);
                Some(pending)
            }
            active => {
                self.queue.in_flight = Some(active);
                None
            }
        }
    }
    pub(super) fn fail_pending(&mut self, pending: Pending<'p, N>, error: Failure) {
        pending.release();
        self.fail(error);
    }
    fn fail(&mut self, error: Failure) {
        self.queue.discard();
        self.queue.storage.failure.set(Some(error));
    }
    pub(super) fn progress(&self) -> State {
        if self.queue.storage.failure().is_some() {
            State::Quiescent
        } else {
            match self.queue.in_flight.as_ref() {
                Some(InFlight::Submitted { .. }) => State::Waiting,
                Some(InFlight::Rejected { .. }) => State::Runnable,
                None if self.queue.current.is_some() || !self.queue.storage.pool.is_empty() => {
                    State::Runnable
                }
                None => State::Quiescent,
            }
        }
    }
    pub(super) fn close(&mut self) {
        self.queue.closed = true;
        self.seal();
    }
    pub(super) fn cancel(&mut self) {
        if matches!(self.progress(), State::Quiescent) {
            return;
        }
        if let Some(active) = self.queue.in_flight.take() {
            let pending = active.cancel();
            pending.release();
        }
        self.fail(Failure::Cancelled);
    }
    fn submit<'borrow>(
        &'borrow mut self,
        pending: Pending<'p, N>,
        driver: &mut dispatch::raw::Context<'_, 'd, dispatch::raw::Retained>,
    ) where
        'd: 'borrow,
    {
        let target = driver
            .driver_ref()
            .targets::<<ID as route::Materialize<kind::Write>>::With>()
            .bind(indices::SlotIndex::ZERO, route::Epoch::INITIAL);
        let bytes = raw::bytes(&pending);
        let op =
            fs::Submission::<<ID as route::Materialize<kind::Write>>::With>::write_current_prefix(
                fd::AsFd::as_fd(&self.file),
                bytes,
                target,
            );
        // SAFETY: pending owns the exact buffer lease, and self retains its file
        // and flight slots. The queue keeps pending through completion or
        // native quiescence of this exact installed appender.
        let result = unsafe { driver.reborrow().submit_file(&self.flights, op) };
        match result {
            Ok(flight) => self.queue.in_flight = Some(InFlight::Submitted { pending, flight }),
            Err(error) => match error.cause() {
                SubmitCause::Full => self.queue.in_flight = Some(InFlight::Rejected { pending }),
                SubmitCause::IdentityExhausted => {
                    self.fail_pending(pending, Failure::Submission(error));
                }
            },
        }
    }
}
impl<'step, 'p, 'd, ID: Identity, const N: usize, const B: usize> Control<'step, 'p, 'd, ID, N, B>
where
    'd: 'step,
{
    pub fn confirmed(&self) -> u64 {
        self.inner.as_ref().get_ref().queue.storage.confirmed()
    }

    pub fn record<'a>(&'a mut self, limit: usize) -> Result<Record<'a, 'd>, WriteError> {
        self.inner.as_mut().get_mut().begin(limit)
    }

    /// Retries the retained suffix after a native write failure.
    pub fn try_retry(&mut self) -> Result<(), Failure> {
        let failure = &self.inner.as_mut().get_mut().queue.storage.failure;
        match failure.get() {
            Some(Failure::System(_)) => failure.set(None),
            Some(error) => return Err(error),
            None => {}
        }
        Ok(())
    }

    /// Seals the current block and replaces the file after successful drain.
    pub fn try_rotate(
        &mut self,
        prepared: Prepared,
    ) -> Result<fs::Offset, Rejected<Prepared, RotateError>> {
        let this = self.inner.as_mut().get_mut();
        if let Some(failure) = this.queue.storage.failure() {
            return Err(Rejected::new(prepared, RotateError::Failed(failure)));
        }
        if this.queue.closed {
            return Err(Rejected::new(prepared, RotateError::Closed));
        }
        this.seal();
        if this.queue.in_flight.is_some() || !this.queue.storage.pool.is_empty() {
            return Err(Rejected::new(prepared, RotateError::Busy));
        }
        let Prepared { file, end } = prepared;
        this.file = file;
        Ok(end)
    }
}

impl<'p, 'd, ID: Identity, const N: usize, const B: usize> event::Drive<'d>
    for Appender<'p, 'd, ID, N, B>
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let (mut app, (), (event, mut driver)) = call.into_parts();
        let done = match event.into_kind() {
            Kind::Write(done) => done,
            event => return ops::ControlFlow::Break(event.into()),
        };
        let this = app.as_mut().get_mut();
        let Some(mut pending) = this.settle(done.token()) else {
            return ops::ControlFlow::Break(Kind::Write(done).into());
        };
        match done.outcome() {
            Ok(transfer::Len::ZERO) => {
                this.fail_pending(pending, Failure::WriteZero);
            }
            Ok(amount) => {
                pending.advance(amount);
                let confirmed = &this.queue.storage.confirmed;
                confirmed.set(confirmed.get() + amount.into_raw() as u64);
                if pending.is_complete() {
                    pending.release();
                } else {
                    this.submit(pending, &mut driver);
                }
            }
            Err(errno) => {
                this.queue.in_flight = Some(InFlight::Rejected { pending });
                this.queue.storage.failure.set(Some(Failure::System(errno)));
            }
        }
        ops::ControlFlow::Continue(())
    }
}

fn options(required_tail: Option<u8>) -> open::Options {
    let access = match required_tail {
        Some(_) => open::Access::ReadWrite,
        None => open::Access::Write,
    };
    open::Options::new(access)
        .creation(open::Creation::Create)
        .append()
        .nofollow()
        .nonblock()
}

fn open_file(path: &path::Path, required_tail: Option<u8>) -> Result<Prepared, OpenError> {
    use fs::path::CStrExt as _;

    let path = Box::try_copy(path)
        .map_err(open::Error::from)
        .map_err(OpenError::Open)?;
    let file = options(required_tail)
        .open(&path)
        .map_err(open::Error::System)
        .map_err(OpenError::Open)?;
    drop(path);
    let (file, end) = file::Locked::try_new(file).map_err(OpenError::Lock)?;
    // Subtracting one admits precisely the nonempty file lengths.
    if let Some(expected) = required_tail
        && let Ok(position) = end.checked_sub(transfer::Len::fixed::<1>())
    {
        let mut tail = [0];
        let observed = file
            .try_read_at(&mut tail, position)
            .map_err(OpenError::Read)?
            .first()
            .copied();
        check_tail(expected, observed)?;
    }
    Ok(Prepared { file, end })
}

pub(in crate::file) fn check_tail(expected: u8, observed: Option<u8>) -> Result<(), OpenError> {
    if observed != Some(expected) {
        return Err(OpenError::Tail { expected, observed });
    }
    Ok(())
}

mod raw;
