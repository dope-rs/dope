use std::ops::ControlFlow;

use dope_core::driver::{retained::flight, route::Identity};
use o3::depot::raw::Proof;

use crate::{
    link::{
        duplex,
        egress::{self, data, reserve::demand},
        slot::{transmit, types},
    },
    wire,
};

use super::{
    Key,
    change::raw::Prepared,
    pending,
    recv::{self, input, storage::queue::Ops as _, storage::tail},
};

pub struct Missing<'a, 'p, 'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w + 'd> {
    pub(super) key: Key<'d, ID>,
    pub(super) runtime: &'a mut W::Runtime<'w, 'd, ID>,
    pub(super) pending: &'p pending::Queue<'d, ID>,
}

impl<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w + 'd> Missing<'_, '_, 'w, 'd, ID, W> {
    pub fn key(&self) -> Key<'d, ID> {
        self.key
    }
}

/// One occupied connection and its original native storage.
pub struct Entry<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode,
> where
    'd: 'w,
{
    pub(super) sender: transmit::Sender<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

impl<'a, 'p, 'w, 'd: 'w, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    D: demand::Mode,
{
    pub fn get(&self) -> &types::Connection<'w, 'd, ID, W, S> {
        self.sender.connection()
    }

    pub fn get_mut(&mut self) -> &mut types::Connection<'w, 'd, ID, W, S> {
        self.sender.slot().0.into_mut()
    }

    pub fn egress_mut(&mut self) -> Option<super::EgressMut<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D>> {
        Some(self.sender()?.view())
    }

    pub(in crate::link::pool) fn receive(&mut self) -> (
        &mut W::Runtime<'w, 'd, ID>,
        &mut types::Connection<'w, 'd, ID, W, S>,
        recv::Borrow<'_, 'd, ID, M::Ingress, input::Receipt<W, M>>,
    ) {
        let key = self.sender.key;
        let Prepared { slab, runtime, .. } = &mut *self.sender.root;
        // SAFETY: this entry retains the admitted slot in its original paired storage.
        let (slot, storage) =
            tail::Slot::from_live(unsafe { Proof::new((slab, key.parts().slot())) });
        // SAFETY: the same entry retains the receive storage paired with this slot.
        let recv = unsafe { Proof::new((storage, key)) }.into();
        (runtime, slot.into_mut(), recv)
    }

    pub fn egress(&self) -> egress::Pending {
        self.sender.root.egress.pending(self.sender.key)
    }

    pub fn pending(&self) -> pending::Handle<'p, 'd, ID> {
        self.sender.root.pending.bound(self.sender.key)
    }

    pub fn sender(
        &mut self,
    ) -> Option<&mut transmit::Sender<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>> {
        self.sender.root.pending.handle(self.sender.key)?;
        Some(&mut self.sender)
    }

    pub fn has_queued(&mut self) -> bool {
        self.sender.has_queued()
    }

    pub fn readiness(&mut self) -> recv::Readiness {
        let (_, slot, recv) = self.receive();
        if slot.engine.operations.recv.is_paused() && !slot.terminal.is_read_done() {
            recv::Readiness::Paused
        } else if recv.storage.has_queued() {
            recv::Readiness::Ready
        } else {
            recv::Readiness::Empty
        }
    }

    pub fn defer(&mut self, chunk: wire::receive::Chunk<'w, 'd, W, wire::receive::mode::Disabled>)
        -> recv::Deferral<'w, 'd, W>
    where
        M::Kind: input::Replay<W>,
    {
        let key = self.sender.key;
        let (slot, storage) = self.sender.slot();
        // SAFETY: the entry keeps this resident paired with its receive storage.
        let mut recv: recv::Borrow<'_, 'd, ID, M::Ingress, input::Receipt<W, M>> =
            unsafe { Proof::new((storage, key)) }.into();
        let (mut entry, tail) = slot.queue::<W, M::Kind>().split();
        let Some(vacant) = tail.vacant() else {
            return recv::Deferral::Occupied(chunk);
        };
        vacant.insert(chunk);
        let slot = entry.get_mut();
        slot.engine.operations.recv.pause();
        if slot.terminal.is_read_done() && slot.engine.operations.recv.has_cancel() {
            recv.rearm.queue();
        } else {
            recv.rearm.remove();
        }
        slot.io().ready_handle().activate();
        recv::Deferral::Ready
    }

    pub fn take_tail(&mut self) -> ControlFlow<(), Option<tail::Taken<'_, 'w, 'd, W>>>
    where
        M::Kind: input::Replay<W>,
    {
        let (slot, _) = self.sender.slot();
        let (entry, tail) = slot.queue::<W, M::Kind>().split();
        let Some(occupied) = tail.occupied() else {
            return ControlFlow::Continue(None);
        };
        if entry.get().is_app_held() {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(Some(tail::Taken::new(occupied)))
    }

    pub fn finish_tail(&mut self)
    where
        M::Kind: input::Replay<W>,
    {
        let (_, slot, mut recv) = self.receive();
        let queued = recv.storage.has_queued();
        let close = slot.engine.operations.lifecycle.is_closing();
        let arm = if slot.terminal.is_read_done() {
            None
        } else {
            slot.engine.operations.recv.resume(close)
        };
        if arm.is_some_and(duplex::Rearm::is_needed) && !queued {
            recv.rearm.queue();
        }
        if queued && !close
            && (slot.terminal.is_read_done() || !slot.engine.operations.recv.is_paused())
        {
            slot.io().ready_handle().activate();
        }
        drop(recv);
        if close {
            self.pending().wake();
        }
    }

    pub fn suspend(&mut self) {
        let key = self.sender.key;
        let (mut entry, storage) = self.sender.slot();
        let slot = entry.get_mut();
        // SAFETY: selection retains the resident and its paired receive storage.
        let mut recv: recv::Borrow<'_, 'd, ID, M::Ingress, input::Receipt<W, M>> =
            unsafe { Proof::new((storage, key)) }.into();
        if slot.engine.operations.recv.pause_app().is_exhausted() {
            recv.rearm.remove();
        }
    }

    pub fn resume(&mut self) {
        let key = self.sender.key;
        let (mut entry, storage) = self.sender.slot();
        let tail = tail::Store::pending(&mut entry);
        let slot = entry.get_mut();
        // SAFETY: selection retains the resident and its paired receive storage.
        let mut recv: recv::Borrow<'_, 'd, ID, M::Ingress, input::Receipt<W, M>> =
            unsafe { Proof::new((storage, key)) }.into();
        let queued = recv.storage.has_queued();
        let close = slot.engine.operations.lifecycle.is_closing();
        let ended = slot.terminal.is_read_done();
        let next = slot.engine.operations.recv.resume_app(close || ended);
        let wake = !close
            && ((queued && (ended || next.is_some() && !slot.engine.operations.recv.is_paused()))
                || (tail && next.is_some())
                || (slot.terminal.is_pending() && next.is_some()));
        let ready = wake.then(|| slot.io().ready_handle());
        if next.is_some_and(duplex::Rearm::is_needed) && !queued {
            recv.rearm.queue();
        }
        if let Some(ready) = ready {
            ready.activate();
        }
    }
}

impl<'a, 'p, 'w, 'd: 'w, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    transmit::Sender<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    D: demand::Mode,
{
    pub fn into_entry(self) -> Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        Entry { sender: self }
    }

    pub(super) fn connection(&self) -> &types::Connection<'w, 'd, ID, W, S> {
        // SAFETY: admission keeps this original root and initialized slot until consumption.
        tail::Table::value(unsafe { Proof::new((&self.root.slab, self.key.parts().slot())) })
    }

    /// Borrows application state and append access while retaining send ownership.
    pub fn view(&mut self) -> super::EgressMut<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D> {
        let (flights, connection, pending, queue) = self.transmit();
        super::EgressMut { flights, connection, pending, queue: queue.reborrow() }
    }

    pub fn has_queued(&mut self) -> bool {
        let key = self.key;
        let (_, storage) = self.slot();
        // SAFETY: the original paired storage remains borrowed for this admitted key.
        let recv: recv::Borrow<'_, 'd, ID, M::Ingress, input::Receipt<W, M>> =
            unsafe { Proof::new((storage, key)) }.into();
        recv.storage.has_queued()
    }

    fn slot(&mut self) -> (
        tail::Slot<'_, types::Connection<'w, 'd, ID, W, S>, ID, input::Tail<'w, 'd, W, M>>,
        &mut recv::State<'d, ID, M::Ingress, input::Receipt<W, M>>,
    ) {
        // SAFETY: the short root reborrow retains the original initialized slot and extents.
        tail::Slot::from_live(unsafe { Proof::new((&mut self.root.slab, self.key.parts().slot())) })
    }

    pub(in crate::link) fn transmit(&mut self) -> (
        &flight::Slots<'d, ID>,
        &mut types::Connection<'w, 'd, ID, W, S>,
        pending::Handle<'_, 'd, ID>,
        egress::Output<'_, 'p, 'd, IOV, B, D>,
    ) {
        let Prepared { slab, flights, pending, egress, .. } = &mut *self.root;
        // SAFETY: the sender retains its original initialized slot for this borrow.
        let (slot, _) = tail::Slot::from_live(unsafe { Proof::new((slab, self.key.parts().slot())) });
        (flights, slot.into_mut(), pending.bound(self.key), egress.bind_queue(self.key))
    }
}
