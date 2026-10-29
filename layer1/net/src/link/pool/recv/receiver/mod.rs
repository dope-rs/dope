use dope_core::driver::route::Identity;
use o3::mem::quota;

use crate::{
    link::pool::recv::storage::tail::Slot,
    wire::receive::{Chunk, mode::Disabled},
};

pub(in crate::link::pool::recv) mod observed;
pub(in crate::link::pool::recv) mod resume;

use dope_core::driver::{
    retained::{self, flight::cancel::Outcome},
    route,
    schedule::maint,
};
use input::Dispatch;
use storage::{queue::Ops as _, tail};

use crate::{
    link::{
        duplex,
        duplex::Cancel,
        egress::reserve::demand,
        event,
        pool::{
            self, pending,
            recv::{Deferral, Failure, Ready, Resumption, arrival, delivery, input, storage},
        },
    },
    wire::{self, open::Runtime as _},
};

pub struct Receiver<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Receiver<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub(in crate::link::pool) fn new(
        pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    ) -> Self {
        Self { pool }
    }

    pub fn arm(
        &mut self,
        key: pool::Key<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), Failure<'d, ID>> {
        let ops = &self.pool.prepared.flights;
        let Some((slot, mut recv)) = self.pool.prepared.slab.get(key.parts()) else {
            return Ok(());
        };
        recv.rearm.submitted(slot.submit_recv(ops, driver))
    }

    pub fn flush(
        &mut self,
        work: maint::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), Failure<'d, ID>> {
        if self.pool.prepared.runtime.is_ready()
            && matches!(
                work.run(|| {
                    self.pool
                        .prepared
                        .runtime
                        .take_ready()
                        .map(|guard| guard.retry())
                }),
                quota::Admission::Exhausted
            )
        {
            return Ok(());
        }
        let count = self.pool.prepared.slab.pending();
        let mut passes = count;
        while passes != 0 {
            passes -= 1;
            let pass = work.run(|| {
                let ops = &self.pool.prepared.flights;
                let slab = &mut self.pool.prepared.slab;
                let key = slab.pop()?;
                let Some((slot, mut recv)) = slab.get(key.parts()) else {
                    return Some(Ok(()));
                };
                if slot.engine.operations.recv.has_cancel() {
                    let engine = &mut slot.engine;
                    let flight = match engine.operations.cancel_recv() {
                        Cancel::Armed(flight) => flight,
                        Cancel::Idle => {
                            recv.rearm.queue();
                            return Some(Ok(()));
                        }
                    };
                    match flight.try_stop(driver.driver()) {
                        Ok(Outcome::Submitted | Outcome::Queued | Outcome::Completed) => {
                            slot.engine.operations.recv.cancel_sent();
                        }
                        Ok(Outcome::Deferred) => recv.rearm.queue(),
                        Err(cause) => {
                            slot.engine.operations.recv.deny_cancel();
                            driver.rejected(cause);
                        }
                    }
                    return Some(Ok(()));
                }
                if !slot.engine.operations.recv.is_armable(slot.is_recv_done()) {
                    return Some(Ok(()));
                }
                Some(recv.rearm.submitted(slot.submit_recv(ops, driver)))
            });
            match pass {
                quota::Admission::Item(result) => result?,
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            }
        }
        Ok(())
    }

    pub fn dispatch<A>(&mut self, item: A) -> event::Dispatch<'d, ID>
    where
        A: arrival::Arrival<'d, Observed = event::Control<'d>>,
        M::Ingress: Dispatch<W>,
    {
        <M::Ingress as Dispatch<W>>::dispatch(self.pool, item.observe())
    }

    pub fn resume(&mut self, target: route::Target<'d, ID>) -> Resumption<'d, ID>
    where
        M::Ingress: Dispatch<W>,
    {
        <M::Ingress as Dispatch<W>>::resume(self.pool, target)
    }

    pub(in crate::link::pool) fn wake_app(&mut self, key: pool::Key<'d, ID>)
    where
        M::Ingress: Dispatch<W>,
    {
        <M::Ingress as Dispatch<W>>::wake(self.pool, key);
    }

    pub fn pop_ready(&mut self, item: &delivery::Resumed<'d, ID>) -> Ready<'d>
    where
        M::Ingress: Dispatch<W>,
    {
        <M::Ingress as Dispatch<W>>::pop(self.pool, item)
    }

    pub fn defer(
        &mut self,
        key: pool::Key<'d, ID>,
        chunk: Chunk<'w, 'd, W, Disabled>,
    ) -> Deferral<'w, 'd, W>
    where
        M::Kind: input::Replay<W>,
    {
        use o3::depot::queue::pool::Vacancy;

        let Some((slot, mut recv)) =
            Slot::receive::<M::Ingress>(&mut self.pool.prepared.slab, key.parts())
        else {
            return Deferral::Unavailable(chunk);
        };
        let (mut entry, chunks) = slot.queue::<W, M::Kind>().split();
        if !chunks.is_empty() {
            return Deferral::Occupied(chunk);
        }
        match chunks.reserve() {
            (Vacancy::Ready(vacant), ()) => vacant.append(chunk),
            (Vacancy::Exhausted, ()) => return Deferral::Full(chunk),
        }
        let slot = entry.get_mut();
        slot.engine.operations.recv.pause();
        if slot.terminal.is_read_done() && slot.engine.operations.recv.has_cancel() {
            recv.rearm.queue();
        } else {
            recv.rearm.remove();
        }
        slot.io().ready_handle().activate();
        Deferral::Ready
    }

    pub fn take_tail<'b>(
        &'b mut self,
        item: &delivery::Resumed<'d, ID>,
    ) -> Option<(pool::Key<'d, ID>, tail::Taken<'b, 'w, 'd, W>)>
    where
        M::Kind: input::Replay<W>,
        'd: 'b,
        'w: 'b,
    {
        let key = item.key();
        let slot = Slot::new(&mut self.pool.prepared.slab, key.parts())?;
        let (_, chunks) = slot.queue::<W, M::Kind>().split();
        Some((key, tail::Taken::new(chunks.front_entry()?.remove())))
    }

    pub fn finish_tail(&mut self, item: &delivery::Resumed<'d, ID>)
    where
        M::Kind: input::Replay<W>,
    {
        let key = item.key();
        let (wake, close) = {
            let slab = &mut self.pool.prepared.slab;
            let Some((slot, mut recv)) = slab.get(key.parts()) else {
                return;
            };
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
            let wake = queued
                && !close
                && (slot.terminal.is_read_done() || !slot.engine.operations.recv.is_paused());
            (wake.then(|| slot.io().ready_handle()), close)
        };
        if let Some(ready) = wake {
            ready.activate();
        }
        if close && let Some(handle) = pending::Pending::of(self.pool).at(key) {
            handle.wake();
        }
    }

    pub fn close_after(&mut self, key: pool::Key<'d, ID>) {
        if let Some(slot) = self.pool.get_mut(key) {
            slot.engine.operations.lifecycle.drain();
        }
    }
}
