use dope_core::driver::route::Identity;
use o3::mem::quota;

pub(in crate::link::pool::recv) mod observed;
pub(in crate::link::pool::recv) mod resume;

use dope_core::driver::{self, retained::flight::cancel::Outcome, route, schedule::maint};
use input::Dispatch;
use observed::{Delivery as _, Observation, Observed};

use crate::{
    link::{
        duplex::Cancel,
        egress::{data, reserve::demand},
        event,
        pool::{
            self,
            recv::{Failure, Ready, Resumption, arrival, delivery, input, reserve},
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
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B: data::raw::Payload<'d>, const IOV: usize, D>
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

    /// Reserves the statically selected ingress policy for one observed data
    /// completion.
    pub fn data<A, O>(
        self,
        done: A,
        capture: impl FnOnce(pool::Key<'d, ID>, &S) -> M::Receipt,
        consume: impl for<'loan> FnOnce(reserve::Reservation<'loan, 'w, 'd, ID, W, M>) -> O,
    ) -> (Option<pool::Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>, O)
    where
        A: arrival::Arrival<'d, input::Receipt<W, M>, Observed = event::Data<'d>>,
        M::Reserve: reserve::Choice<W, M::Ingress>,
        input::Receipt<W, M>: 'a,
    {
        let (event, timing) = done.observe().into_parts();
        let key = match self.pool.keys.parse(event.token()) {
            Ok(key) => key.connection(),
            Err(_) => {
                drop(event);
                return (None, consume(event::Reservation::Drop));
            }
        };
        let mut entry = match self.pool.entry(key) {
            Ok(entry) => entry,
            Err(_) => {
                drop(event);
                return (None, consume(event::Reservation::Drop));
            }
        };
        let next = match reserve::Plan::observe(&mut entry, event, timing, capture) {
            Ok(plan) => <M::Reserve as reserve::Choice<W, M::Ingress>>::reserve(plan),
            Err(next) => next,
        };
        let output = consume(next);
        (Some(entry), output)
    }

    pub fn arm(
        &mut self,
        key: pool::Key<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(), Failure<'d, ID>> {
        let ops = &self.pool.prepared.flights;
        let Some((slot, mut recv)) = self.pool.prepared.slab.get_mut(key.parts()) else {
            return Ok(());
        };
        recv.rearm.submitted(slot.submit_recv(ops, driver))
    }

    #[inline]
    pub fn flush(
        &mut self,
        work: maint::Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
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
        if count == 0 {
            return Ok(());
        }
        self.flush_pending(count, work, driver)
    }

    #[inline(never)]
    fn flush_pending(
        &mut self,
        mut passes: usize,
        work: maint::Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(), Failure<'d, ID>> {
        while passes != 0 {
            passes -= 1;
            let pass = work.run(|| {
                let ops = &self.pool.prepared.flights;
                let slab = &mut self.pool.prepared.slab;
                let key = slab.pop()?;
                let Some((slot, mut recv)) = slab.get_mut(key.parts()) else {
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
                    match flight.try_stop(driver) {
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

    pub fn dispatch<A>(
        self,
        item: A,
    ) -> (
        Option<pool::Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>,
        event::Dispatch<'d, ID>,
    )
    where
        A: arrival::Arrival<'d, Observed = event::Control<'d>>,
        M::Ingress: Dispatch<W>,
    {
        let (item, timing) = item.observe().into_parts();
        let key = match self.pool.keys.parse(item.token()) {
            Ok(key) => key.connection(),
            Err(_) => return (None, event::Dispatch::Retired(item.retire())),
        };
        let mut entry = match self.pool.entry(key) {
            Ok(entry) => entry,
            Err(_) => return (None, event::Dispatch::Retired(item.retire())),
        };
        let next = match Observed::<ID, W, S, M>::observe(&mut entry, item, timing) {
            Observation::Complete(done) => done,
            Observation::Ready(next, storage) => {
                <M::Ingress as Dispatch<W>>::Delivery::<'_, 'd, input::Receipt<W, M>>::ready(
                    next, storage,
                )
            }
        };
        (Some(entry), next)
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

    pub fn pop_ready(&mut self, item: &delivery::Resumed<'d, ID>) -> Ready<'d, input::Receipt<W, M>>
    where
        M::Ingress: Dispatch<W>,
    {
        <M::Ingress as Dispatch<W>>::pop(self.pool, item)
    }
}
