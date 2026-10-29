use std::marker;

use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::{app::batch::Bound, ready},
};
use o3::mem;
use receive::{batch, mode, reserve};
use slot::recv::{Blocked, Prepared};
use storage::queue::{Push, Write};

use crate::{
    link::{
        event,
        pool::{
            self,
            recv::{self, input, storage},
        },
        slot,
    },
    wire::{
        self, receive,
        receive::{Chunk, mode::Disabled},
    },
};

/// One admitted receive event and its inseparable connection transaction.
pub struct Data<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w + 'd, M: input::Kind<W>>
where
    'd: 'w,
{
    prepared: Prepared<'a, 'w, 'd, ID, W, M::Ingress>,
    rearm: recv::Rearm<'a, 'd, ID>,
    event: event::Data<'d>,
    input: marker::PhantomData<M>,
}

#[must_use]
pub struct Resumed<'d, ID: Identity> {
    key: pool::Key<'d, ID>,
    ready: ready::Key<'d>,
}

impl<'d, ID: Identity> Resumed<'d, ID> {
    pub(in crate::link::pool) const fn new(key: pool::Key<'d, ID>, ready: ready::Key<'d>) -> Self {
        Self { key, ready }
    }

    pub const fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }

    pub const fn ready(&self) -> ready::Key<'d> {
        self.ready
    }
}

pub(super) struct Park<
    'a,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w + 'd,
    P: storage::raw::Mode,
    R: reserve::Try<W>,
> where
    'd: 'w,
    'w: 'a,
    P::Bound<'a, 'd>: Write<'d>,
{
    store: P::Bound<'a, 'd>,
    rearm: recv::Rearm<'a, 'd, ID>,
    wait: Blocked<'a, 'w, 'd, ID, W, R>,
    event: event::Data<'d>,
}

pub trait BorrowedData {}

impl BorrowedData for input::Borrowed {}
impl BorrowedData for input::Coupled {}

pub trait ScopedData {}

impl ScopedData for input::Scoped {}

pub trait Ownership<W: input::Execution>: mode::Credit {
    fn own<'a, 'w, 'd, ID: Identity + 'w>(
        data: Data<'a, 'w, 'd, ID, W, input::Retained>,
    ) -> event::DispatchRecv<'d, ID, Chunk<'w, 'd, W>>
    where
        'd: 'w,
        W: 'w + 'd;
}

impl<W> Ownership<W> for mode::Disabled
where
    W: input::Execution,
    W::Receive: receive::Policy<W, Credit = Self>,
{
    fn own<'a, 'w, 'd, ID: Identity + 'w>(
        data: Data<'a, 'w, 'd, ID, W, input::Retained>,
    ) -> event::DispatchRecv<'d, ID, Chunk<'w, 'd, W>>
    where
        'd: 'w,
        W: 'w + 'd,
    {
        let next = data.prepared.scoped(data.event.into_lease());
        Data::<ID, W, input::Retained>::finish(data.rearm, next)
    }
}

impl<W> Ownership<W> for mode::Enabled
where
    W: input::Execution,
    W::Receive: receive::Policy<W, Credit = Self>,
    W::Retained: mode::Stored,
{
    fn own<'a, 'w, 'd, ID: Identity + 'w>(
        data: Data<'a, 'w, 'd, ID, W, input::Retained>,
    ) -> event::DispatchRecv<'d, ID, Chunk<'w, 'd, W>>
    where
        'd: 'w,
        W: 'w + 'd,
    {
        let next = data.prepared.retained(data.event.into_lease());
        Data::<ID, W, input::Retained>::finish(data.rearm, next)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, P, R> Park<'a, 'w, 'd, ID, W, P, R>
where
    'd: 'w,
    'w: 'a,
    W: wire::Spec + 'w + 'd,
    P: storage::raw::Mode,
    R: reserve::Try<W>,
    P::Bound<'a, 'd>: Write<'d>,
{
    pub(super) fn new(
        store: P::Bound<'a, 'd>,
        rearm: recv::Rearm<'a, 'd, ID>,
        wait: Blocked<'a, 'w, 'd, ID, W, R>,
        event: event::Data<'d>,
    ) -> Self {
        Self {
            store,
            rearm,
            wait,
            event,
        }
    }

    pub(super) fn park(mut self) -> event::ParkRecv<'d, ID> {
        let wait = self.wait;
        let key = wait.resident.key();
        let reg = match reserve::Wait::register(wait.block, wait.credit) {
            Ok(reg) => reg,
            Err(cause) => {
                return event::ParkRecv::Failed {
                    key,
                    error: event::ParkError::Register(cause),
                };
            }
        };
        let done = self.event;
        match self.store.prepare_push(done) {
            Push::Ready { vacant, done } => {
                vacant.append(done.complete());
                reserve::Registration::commit(reg);
                if wait.needs_cancel {
                    self.rearm.queue();
                }
                event::ParkRecv::Deferred
            }
            Push::Full(..) => event::ParkRecv::Failed {
                key,
                error: event::ParkError::Full,
            },
        }
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, M> Data<'a, 'w, 'd, ID, W, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Kind<W>,
{
    pub(super) fn new(
        prep: Prepared<'a, 'w, 'd, ID, W, M::Ingress>,
        rearm: recv::Rearm<'a, 'd, ID>,
        event: event::Data<'d>,
    ) -> Self {
        Self {
            prepared: prep,
            rearm,
            event,
            input: marker::PhantomData,
        }
    }

    pub(in crate::link::pool::recv) fn finish<C>(
        mut rearm: recv::Rearm<'_, 'd, ID>,
        next: slot::Decision<C>,
    ) -> event::DispatchRecv<'d, ID, C> {
        let key = rearm.key();
        match next {
            slot::Decision::Control(ctl) => event::DispatchRecv::Control(Self::control(rearm, ctl)),
            slot::Decision::Chunk {
                chunk,
                needs_rearm: arm,
            } => {
                if arm {
                    rearm.queue();
                }
                event::DispatchRecv::Chunk(key, chunk)
            }
        }
    }

    pub(in crate::link::pool::recv) fn control(
        mut rearm: recv::Rearm<'_, 'd, ID>,
        ctl: slot::Control,
    ) -> event::ControlRecv<'d, ID> {
        let key = rearm.key();
        match ctl {
            slot::Control::Eof => event::ControlRecv::Eof(key),
            slot::Control::Cancelled => event::ControlRecv::Cancelled(key),
            slot::Control::Failed(error) => event::ControlRecv::Failed { key, error },
            slot::Control::Credit {
                cause,
                needs_rearm: arm,
            } => {
                if arm {
                    rearm.queue();
                }
                event::ControlRecv::Credit { key, cause }
            }
            slot::Control::Discarded {
                needs_rearm: arm,
                finished,
            } => {
                if arm {
                    rearm.queue();
                }
                event::ControlRecv::Discarded { key, finished }
            }
            slot::Control::NoChunk { needs_rearm: arm } => {
                if arm {
                    rearm.queue();
                }
                event::ControlRecv::NoChunk(key)
            }
        }
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, M> Data<'a, 'w, 'd, ID, W, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Kind<W> + BorrowedData,
{
    /// Moves the admitted event into caller-owned backing before borrowing its
    /// bytes. The returned batch retains that backing, not the pool loan.
    pub fn dispatch<'bytes, 'id>(
        self,
        backing: &'bytes mut Option<event::Data<'d>>,
        cap: Bound<'id, batch::Capacity<'d, W>>,
    ) -> (
        event::DispatchRecv<'d, ID, batch::Items<'bytes, W>>,
        mem::raw::Unused<'id>,
    )
    where
        'w: 'bytes,
    {
        let event = backing.insert(self.event);
        let (next, unused) = self.prepared.data(event.lease_mut(), cap);
        (Self::finish(self.rearm, next), unused)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, M> Data<'a, 'w, 'd, ID, W, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Kind<W> + ScopedData,
{
    pub fn scoped(self) -> event::DispatchRecv<'d, ID, Chunk<'w, 'd, W, Disabled>> {
        let next = self.prepared.scoped(self.event.into_lease());
        Self::finish(self.rearm, next)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W> Data<'a, 'w, 'd, ID, W, input::Retained>
where
    'd: 'w,
    W: input::Execution + 'w + 'd,
    <W::Receive as receive::Policy<W>>::Credit: Ownership<W>,
{
    pub fn retained(self) -> event::DispatchRecv<'d, ID, Chunk<'w, 'd, W>> {
        <<W::Receive as receive::Policy<W>>::Credit as Ownership<W>>::own(self)
    }
}

pub(super) fn defer_completion<'a, 'd: 'a, ID: Identity, P: storage::raw::Mode>(
    mut store: P::Bound<'a, 'd>,
    mut rearm: recv::Rearm<'_, 'd, ID>,
    resident: recv::Resident<'_, 'd, ID>,
    cancel: bool,
    done: event::Data<'d>,
) -> event::ParkRecv<'d, ID>
where
    P::Bound<'a, 'd>: Write<'d>,
{
    let key = resident.key();
    match store.prepare_push(done) {
        Push::Ready { vacant, done } => {
            vacant.append(done.complete());
            if cancel {
                rearm.queue();
            }
            event::ParkRecv::Deferred
        }
        Push::Full(..) => event::ParkRecv::Failed {
            key,
            error: event::ParkError::Full,
        },
    }
}

const _: () = assert!(
    size_of::<Resumed<'static, KeyTag<0>>>()
        == size_of::<(pool::Key<'static, KeyTag<0>>, ready::Key<'static>)>()
);
