use dope_core::driver::{
    receive::credit,
    route::{self, Identity, table::entries::occupied},
};
use storage::queue::Ops;

use crate::{
    link::{
        duplex,
        egress::reserve::demand,
        pool::{
            self, pending,
            recv::{self, Missing, delivery, input, storage},
        },
        slot::types,
    },
    wire::{
        self,
        receive::{self, mode, reserve},
    },
};

pub(in crate::link::pool::recv) struct Resume<
    'a,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
> where
    'd: 'w,
{
    entry: occupied::Entry<'a, types::Connection<'w, 'd, ID, W, S>, ID>,
    runtime: &'a mut W::Runtime<'w, 'd, ID>,
    recv: recv::Borrow<'a, 'd, ID, M::Ingress>,
    pending: &'a pending::Queue<'d, ID>,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M> Resume<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
{
    pub(in crate::link::pool::recv) fn try_new<'p, T, B, const IOV: usize, D>(
        pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        target: route::Target<'d, ID>,
    ) -> Result<Self, Missing<'d, ID>>
    where
        T: crate::Transport,
        D: demand::Mode,
    {
        let key = pool::Key::from_target(target);
        let (entry, storage) = pool
            .prepared
            .slab
            .try_entry(key.parts())
            .map_err(|cause| Missing { key, cause })?;
        Ok(Self {
            entry,
            runtime: &mut pool.prepared.runtime,
            recv: storage,
            pending: pool.prepared.pending,
        })
    }

    pub(in crate::link::pool::recv) fn direct(mut self) -> delivery::Resumed<'d, ID> {
        self.resume();
        let slot = self.entry.get_mut();
        delivery::Resumed::new(slot.key, slot.io().ready_key())
    }

    pub(in crate::link::pool::recv) fn credit(mut self) -> delivery::Resumed<'d, ID>
    where
        M: input::Mode<W, Ingress = mode::Enabled>,
    {
        let wake = self.prepare();
        self.finish(wake)
    }

    pub(in crate::link::pool::recv) fn waiting<P>(mut self) -> delivery::Resumed<'d, ID>
    where
        M: input::Mode<W, Ingress = mode::Waiting<P>>,
        P: reserve::Try<W>,
        W::Receive: receive::Policy<W, Reserve = reserve::Waiting<P>>,
    {
        let wake = self.prepare();
        match &wake {
            credit::Take::Ready(credit::Wake::Returned) => P::release_recv(self.runtime),
            credit::Take::Ready(credit::Wake::Retry) | credit::Take::Idle => {}
        }
        self.finish(wake)
    }

    fn resume(&mut self) {
        let slot = self.entry.get_mut();
        if !slot.is_recv_done()
            && slot
                .control
                .engine
                .operations
                .recv
                .exhaustion()
                .try_resume()
                .is_started()
        {
            self.recv.rearm.queue();
        }
    }

    fn prepare(&mut self) -> credit::Take
    where
        M::Ingress: storage::raw::Mode<Bound<'a, 'd> = storage::Loan<'a, 'd>>,
    {
        self.resume();
        let slot = self.entry.get_mut();
        let wake = self.recv.storage.get().take();
        match &wake {
            credit::Take::Ready(credit::Wake::Returned)
            | credit::Take::Ready(credit::Wake::Retry) => {
                let close = slot.control.engine.operations.lifecycle.is_closing();
                let queued = self.recv.storage.has_queued();
                if !slot.terminal.is_read_done()
                    && slot
                        .control
                        .engine
                        .operations
                        .recv
                        .resume(close)
                        .is_some_and(duplex::Rearm::is_needed)
                    && !queued
                {
                    self.recv.rearm.queue();
                }
            }
            credit::Take::Idle => {}
        }
        wake
    }

    fn finish(self, wake: credit::Take) -> delivery::Resumed<'d, ID> {
        let slot = self.entry.get();
        match wake {
            credit::Take::Ready(credit::Wake::Returned)
            | credit::Take::Ready(credit::Wake::Retry) => {
                if slot.control.engine.operations.lifecycle.is_closing()
                    && let Some(handle) = self.pending.handle(slot.key)
                {
                    handle.wake();
                }
            }
            credit::Take::Idle => {}
        }
        delivery::Resumed::new(slot.key, slot.io().ready_key())
    }
}
