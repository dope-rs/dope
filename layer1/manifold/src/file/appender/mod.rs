use std::{marker, ops, os::fd, path, pin};

use dope_core::{
    driver::{
        self,
        result::SubmitError,
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
};
pub use record::Record;
use state::State;
pub use storage::Storage;

use crate::{
    app::{creation, dispatch, dispatch::event},
    file,
};

mod block;
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
    Read(dope_core::Errno),
    Tail { expected: u8, observed: Option<u8> },
}
#[derive(Clone, Copy, Debug)]
pub enum Failure {
    System(dope_core::Errno),
    Submission(SubmitError),
    WriteZero,
    Amount {
        observed: transfer::Len,
        remaining: transfer::Len,
    },
    Extent(fs::Extent),
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
    file: lease::Owner<'d, ID, file::Locked>,
    pub(super) flights: flight::Slots<'d, <ID as route::Materialize<kind::Write>>::With>,
    offset: fs::Offset,
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
    pub fn open(
        path: impl AsRef<path::Path>,
        required_tail: Option<u8>,
        storage: &'p mut Storage<N, B>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Self, creation::Error> {
        use fs::path::CStrExt as _;

        let path = Box::try_copy(path.as_ref())
            .map_err(open::Error::from)
            .map_err(OpenError::Open)
            .map_err(creation::Error::Appender)?;
        let access = match required_tail {
            Some(_) => open::Access::ReadWrite,
            None => open::Access::Write,
        };
        let file = open::Options::new(access)
            .creation(open::Creation::Create)
            .nofollow()
            .nonblock()
            .open(&path)
            .map_err(open::Error::System)
            .map_err(OpenError::Open)
            .map_err(creation::Error::Appender)?;
        drop(path);
        let (file, offset) = file::Locked::try_new(file)
            .map_err(OpenError::Lock)
            .map_err(creation::Error::Appender)?;
        // Subtracting one admits precisely the nonempty file lengths.
        if let Some(expected) = required_tail
            && let Ok(position) = offset.checked_sub(transfer::Len::fixed::<1>())
        {
            let mut tail = [0];
            let observed = file
                .try_read_at(&mut tail, position)
                .map_err(OpenError::Read)
                .map_err(creation::Error::Appender)?
                .first()
                .copied();
            if observed != Some(expected) {
                return Err(creation::Error::Appender(OpenError::Tail {
                    expected,
                    observed,
                }));
            }
        }
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
        let file = held.into_claim().store(file);
        storage.failure.set(None);
        Ok(Self {
            flights,
            file,
            offset,
            queue: Queue {
                storage,
                current: None,
                in_flight: None,
                closed: false,
            },
        })
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
            if limit > B - block.get().bytes.len() {
                block.push_back(());
                continue;
            }
            let block = &mut self.queue.current.insert(block).get_mut().bytes;
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
        if block.get().bytes.is_empty() {
            block.release();
        } else {
            block.push_back(());
        }
    }
    pub(super) fn prepare(&mut self) -> Option<Pending<'p, N, B>> {
        self.seal();
        if let Some(active) = self.queue.in_flight.take() {
            return match active {
                InFlight::Rejected { pending } => Some(pending),
                active @ InFlight::Submitted { .. } => {
                    self.queue.in_flight = Some(active);
                    None
                }
            };
        }
        self.queue
            .storage
            .pool
            .pop_front()
            .map(|(block, ())| Pending::new(block))
    }
    pub(super) fn install(
        &mut self,
        pending: Pending<'p, N, B>,
        result: Result<flight::Flight<'d, kind::Write>, SubmitError>,
    ) {
        match InFlight::try_new(pending, result) {
            Ok(active) => self.queue.in_flight = Some(active),
            Err((pending, error)) => self.fail_pending(pending, error),
        }
    }
    pub(super) fn settle(
        &mut self,
        target: route::Erased<'d, kind::Write>,
    ) -> Option<Pending<'p, N, B>> {
        let active = self.queue.in_flight.take()?;
        match active.complete(target) {
            Ok(pending) => Some(pending),
            Err(active) => {
                self.queue.in_flight = Some(active);
                None
            }
        }
    }
    pub(super) fn complete(&mut self, pending: Pending<'p, N, B>) {
        let offset = self.offset.checked_add(pending.length());
        pending.release();
        match offset {
            Ok(offset) => self.offset = offset,
            Err(extent) => self.fail(Failure::Extent(extent)),
        }
    }
    pub(super) fn fail_pending(&mut self, pending: Pending<'p, N, B>, error: Failure) {
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
    fn submit<'borrow, 'scope>(
        &'borrow mut self,
        pending: Pending<'p, N, B>,
        driver: &mut dispatch::raw::Context<'_, 'scope, 'd, dispatch::raw::Retained>,
    ) where
        'd: 'scope,
        'scope: 'borrow,
    {
        let written = pending.written();
        let offset = match self.offset.checked_add(written) {
            Ok(offset) => offset,
            Err(extent) => {
                self.fail_pending(pending, Failure::Extent(extent));
                return;
            }
        };
        let target = driver
            .driver()
            .driver_ref()
            .targets::<<ID as route::Materialize<kind::Write>>::With>()
            .bind(indices::SlotIndex::ZERO, route::Epoch::INITIAL);
        let bytes = raw::bytes(&pending);
        let op = fs::Submission::<<ID as route::Materialize<kind::Write>>::With>::write_prefix(
            fd::AsFd::as_fd(&*self.file),
            bytes,
            offset,
            target,
        );
        // SAFETY: pending owns the exact buffer lease, and self retains its file
        // and flight slots. install keeps pending through completion or native
        // quiescence of this exact installed appender.
        let result = unsafe { driver.reborrow().submit_file(&self.flights, op) };
        self.install(pending, result);
    }
}
impl<'step, 'p, 'd, ID: Identity, const N: usize, const B: usize> Control<'step, 'p, 'd, ID, N, B>
where
    'd: 'step,
{
    pub fn record<'a>(&'a mut self, limit: usize) -> Result<Record<'a, 'd>, WriteError> {
        self.inner.as_mut().get_mut().begin(limit)
    }
}

const _: () = assert!(
    size_of::<lease::Owner<'static, route::KeyTag<0>, file::Locked>>() == 2 * size_of::<usize>()
);

impl<'p, 'd, ID: Identity, const N: usize, const B: usize> event::Drive<'d>
    for Appender<'p, 'd, ID, N, B>
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
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
                if let Err(left) = pending.advance(amount) {
                    this.fail_pending(
                        pending,
                        Failure::Amount {
                            observed: amount,
                            remaining: left,
                        },
                    );
                    return ops::ControlFlow::Continue(());
                }
                if pending.is_complete() {
                    this.complete(pending);
                } else {
                    this.submit(pending, &mut driver);
                }
            }
            Err(errno) => this.fail_pending(pending, Failure::System(errno)),
        }
        ops::ControlFlow::Continue(())
    }
}

mod raw;
