use std::mem;

use dope_core::driver::{
    retained::{self, flight},
    route::{self, Identity, KeyTag, kind},
};
use send::{Store as _, reclaim};

use crate::{
    link::{
        duplex::{self, sending, state},
        egress::{self, data, reserve::demand},
        event,
        pool::{self, pending},
        slot::{
            transmit::{Attempt, Invalid, graceful, outcome},
            types,
        },
    },
    wire::{self, send, terminal},
};

/// One connection, its queued owners, and its retained send reservation.
pub struct Sender<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w,
    S,
    B,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> {
    pub(in crate::link) flights: &'a flight::Slots<'d, ID>,
    pub(in crate::link) connection: &'a mut types::Connection<'w, 'd, ID, W, S>,
    pub(in crate::link) pending: pending::Handle<'a, 'd, ID>,
    pub(in crate::link) queue: egress::Output<'a, 'p, 'd, IOV, B, D>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, W, S, B, const IOV: usize, D>
    Sender<'a, 'p, 'w, 'd, ID, W, S, B, IOV, D>
where
    W: wire::Spec + 'w,
    B: data::raw::Payload<'d>,
    D: demand::Mode,
{
    /// Borrows application state and append access while retaining send ownership.
    pub fn view(&mut self) -> pool::EgressMut<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D> {
        pool::EgressMut {
            flights: self.flights,
            connection: self.connection,
            pending: self.pending,
            queue: self.queue.reborrow(),
        }
    }

    pub fn submit(&mut self, driver: &mut retained::Context<'_, '_, 'd>) -> Attempt {
        let outcome = self.run(driver);
        self.pending
            .wait_send(self.connection.send_status().is_waiting());
        outcome
    }

    fn run(&mut self, driver: &mut retained::Context<'_, '_, 'd>) -> Attempt {
        let target = self.connection.key().target();
        let slot = &mut *self.connection;
        let queue = &mut self.queue;
        let slots = self.flights;
        if queue.is_active() {
            return Attempt::Waiting;
        }
        let end = slot.control.engine.send(&mut slot.control.terminal);
        let fd = slot.control.engine.establish.fd();
        let Some(submit) = slot
            .control
            .engine
            .operations
            .submission(sending::Source::Queued)
            .try_ready()
        else {
            return Attempt::Waiting;
        };
        let plan = match queue.prepare() {
            egress::Preparation::Ready(plan) => plan,
            egress::Preparation::Empty => {
                return match graceful::flush(slot, slots, driver, target, sending::Source::Queued) {
                    outcome::Progress::Submitted => Attempt::Waiting,
                    outcome::Progress::Rejected(error) => {
                        slot.discard();
                        Attempt::Rejected(event::SendFailure::Queued(error), reclaim::Advance::Idle)
                    }
                    outcome::Progress::Idle => Self::shutdown(slot, slots, driver, target),
                };
            }
        };
        let end = match end {
            terminal::Admission::Open(end) => end,
            terminal::Admission::Closed => return Attempt::Waiting,
        };
        let Some(fd) = fd else {
            return Attempt::Waiting;
        };
        let mut flight = match plan.reserve(&driver.regions().0) {
            egress::Reservation::Ready(flight) => flight,
            egress::Reservation::Active | egress::Reservation::Unavailable => {
                return Attempt::Waiting;
            }
        };
        let (submit, captured) = {
            let plain = flight.vectored();
            let context = send::Context::new(&mut slot.wire, send::Storage::new(&mut slot.send));
            let done = <W::Send as send::Policy<W>>::prepare_many::<ID, IOV>(context, plain, end);
            let (ready, captured) = done.into_parts();
            let ready = captured.resolve(ready, send::Prepared::close_after);
            let submit = submit.submit(fd, slots, driver, target, ready);
            (submit, captured)
        };
        slot.control.engine.captured(captured);
        let (credit, submit) = submit.into_parts();
        let advance =
            <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::advance(
                credit,
            );
        let released = match submit {
            send::State::Submitted => slot.control.engine.operations.queue_submit(credit),
            send::State::Rejected(_) | send::State::Idle => slot
                .control
                .engine
                .operations
                .queue_unsent(credit)
                .map(reclaim::Submission::Reclaim),
        };
        let free = match released {
            Ok(reclaim::Submission::Reclaim(free)) => free,
            Ok(reclaim::Submission::Retain) => {
                flight.retain(target.operation::<kind::Send>().erase_route());
                return Attempt::Waiting;
            }
            Err(error) => {
                slot.discard();
                return Attempt::Invalid(error.into());
            }
        };
        if let Err(error) = flight.release(free, driver.region_token()) {
            slot.discard();
            return Attempt::Invalid(Invalid::Egress(error));
        }
        if let send::State::Rejected(error) = submit {
            slot.discard();
            return Attempt::Rejected(event::SendFailure::Queued(error), advance);
        }
        match advance {
            reclaim::Advance::Ready if !queue.is_empty() => Attempt::Runnable(advance),
            _ => Attempt::Quiescent(advance),
        }
    }

    fn shutdown(
        slot: &mut types::Connection<'w, 'd, ID, W, S>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
        target: route::Target<'d, ID>,
    ) -> Attempt {
        if !slot.control.engine.operations.lifecycle.close.is_shutting() {
            return Attempt::Quiescent(reclaim::Advance::Idle);
        }
        match slot.control.engine.operations.lifecycle.close.request() {
            state::Request::Ready | state::Request::Duplicate => {}
            state::Request::Discarded => return Attempt::Quiescent(reclaim::Advance::Idle),
        }
        let Some(fd) = slot.control.engine.establish.fd() else {
            return Attempt::Waiting;
        };
        let end = slot.control.engine.send(&mut slot.control.terminal);
        let submit = slot
            .control
            .engine
            .operations
            .submission(sending::Source::Queued);
        let (captured, handoff) = graceful::submit_pending(
            send::Context::new(&mut slot.wire, send::Storage::new(&mut slot.send)),
            submit,
            fd,
            slots,
            driver,
            target,
            end,
        );
        if matches!(captured, terminal::Captured::Terminal) {
            let handle = slot.control.engine.establish.ready_handle();
            slot.control.engine.operations.capture(handle);
        }
        if let outcome::Progress::Rejected(error) = handoff.progress() {
            slot.discard();
            return Attempt::Rejected(event::SendFailure::Queued(error), reclaim::Advance::Idle);
        }
        if slot.control.engine.operations.sending.is_inflight()
            || slot.control.engine.operations.lifecycle.is_waiting()
        {
            return Attempt::Waiting;
        }
        if !slot.send.as_slice().is_empty() {
            return Attempt::Runnable(reclaim::Advance::Idle);
        }
        match slot
            .control
            .engine
            .operations
            .shutdown(driver, slots, fd, target)
        {
            duplex::Shutdown::Armed | duplex::Shutdown::Busy | duplex::Shutdown::Submitted => {
                Attempt::Waiting
            }
            duplex::Shutdown::Complete => Attempt::Quiescent(reclaim::Advance::Idle),
            duplex::Shutdown::Rejected(error) => {
                slot.discard();
                Attempt::Rejected(
                    event::SendFailure::WriteShutdown(error),
                    reclaim::Advance::Idle,
                )
            }
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<
            Sender<
                'static,
                'static,
                'static,
                'static,
                KeyTag<0>,
                wire::Identity,
                (),
                &'static [u8],
                32,
            >,
        >() == 6 * mem::size_of::<usize>()
            + mem::size_of::<pending::Handle<'static, 'static, KeyTag<0>>>()
    );
};
