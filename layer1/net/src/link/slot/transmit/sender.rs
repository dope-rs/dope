use std::mem;

use dope_core::driver::{
    self,
    retained::flight,
    route::{self, Identity, KeyTag},
};
use send::{Store as _, reclaim};

use crate::{
    link::{
        duplex::{self, sending, state},
        egress::{self, data, reserve::demand},
        event,
        pool::{self, recv::input},
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
    'd: 'w,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> {
    pub(in crate::link) root: &'a mut pool::change::raw::Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    pub(in crate::link) key: pool::Key<'d, ID>,
}

impl<
    'a,
    'p,
    'w,
    'd: 'w,
    ID: Identity + 'w,
    T,
    W,
    S,
    M,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D,
> Sender<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub fn submit(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
        release: impl FnMut(B),
    ) -> Attempt {
        let (flights, connection, pending, mut queue) = self.transmit();
        let outcome = run(connection, &mut queue, flights, driver, release);
        pending.wait_send(connection.send_status().is_waiting());
        outcome
    }
}

fn run<'w, 'd: 'w, ID: Identity + 'w, W, S, B: data::raw::Payload<'d>, const IOV: usize, D>(
    connection: &mut types::Connection<'w, 'd, ID, W, S>,
    queue: &mut egress::Output<'_, '_, 'd, IOV, B, D>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut driver::Context<'_, 'd>,
    release: impl FnMut(B),
) -> Attempt
where
    W: wire::Spec + 'w,
    D: demand::Mode,
{
    let target = connection.key().target();
    let slot = &mut connection.control;
    if queue.is_active() {
        return Attempt::Waiting;
    }
    let end = slot.engine.send(&mut slot.terminal);
    let fd = slot.engine.establish.fd();
    let Some(submit) = slot
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
                outcome::Progress::Idle => shutdown(slot, slots, driver, target),
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
        let ready = ready.resolve(captured);
        let submit = submit.submit(fd, slots, driver, target, ready);
        (submit, captured)
    };
    slot.engine.captured(captured);
    let (credit, submit) = submit.into_parts();
    let advance =
        <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::advance(credit);
    match submit {
        send::State::Submitted => {
            let prefix = match slot.engine.operations.queue_submit(credit) {
                Ok(prefix) => prefix,
                Err(error) => {
                    flight.retain();
                    slot.discard();
                    return Attempt::Invalid(error.into());
                }
            };
            if let Err(error) = flight.try_retain(prefix) {
                slot.discard();
                return Attempt::Invalid(Invalid::Egress(error));
            }
            return Attempt::Waiting;
        }
        send::State::Rejected(error) => {
            slot.discard();
            return Attempt::Rejected(event::SendFailure::Queued(error), advance);
        }
        send::State::Idle if matches!(captured, terminal::Captured::Terminal) => {
            return Attempt::Waiting;
        }
        send::State::Idle => {}
    }
    let free = match slot.engine.operations.queue_unsent(credit) {
        Ok(free) => free,
        Err(error) => {
            slot.discard();
            return Attempt::Invalid(error.into());
        }
    };
    if let Err(error) = flight.release(free, driver.region_token(), release) {
        slot.discard();
        return Attempt::Invalid(Invalid::Egress(error));
    }
    match advance {
        reclaim::Advance::Ready if !queue.is_empty() => Attempt::Runnable(advance),
        _ => Attempt::Quiescent(advance),
    }
}

fn shutdown<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w>(
    slot: &mut types::Control<'w, 'd, ID, W>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut driver::Context<'_, 'd>,
    target: route::Target<'d, ID>,
) -> Attempt {
    if !slot.engine.operations.lifecycle.close.is_shutting() {
        return Attempt::Quiescent(reclaim::Advance::Idle);
    }
    match slot.engine.operations.lifecycle.close.request() {
        state::Request::Ready | state::Request::Duplicate => {}
        state::Request::Discarded => return Attempt::Quiescent(reclaim::Advance::Idle),
    }
    let Some(fd) = slot.engine.establish.fd() else {
        return Attempt::Waiting;
    };
    let end = slot.engine.send(&mut slot.terminal);
    let submit = slot.engine.operations.submission(sending::Source::Queued);
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
        let handle = slot.engine.establish.ready_handle();
        slot.engine.operations.capture(handle);
    }
    if let outcome::Progress::Rejected(error) = handoff.progress() {
        slot.discard();
        return Attempt::Rejected(event::SendFailure::Queued(error), reclaim::Advance::Idle);
    }
    if slot.engine.operations.sending.is_inflight() || slot.engine.operations.lifecycle.is_waiting()
    {
        return Attempt::Waiting;
    }
    if !slot.send.as_slice().is_empty() {
        return Attempt::Runnable(reclaim::Advance::Idle);
    }
    match slot.engine.operations.shutdown(driver, slots, fd, target) {
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

const _: () = {
    assert!(
        mem::size_of::<
            Sender<
                'static,
                'static,
                'static,
                'static,
                KeyTag<0>,
                crate::tcp::Tcp,
                wire::Identity,
                (),
                input::Borrowed,
                &'static [u8],
                32,
            >,
        >() == mem::size_of::<usize>() + mem::size_of::<pool::Key<'static, KeyTag<0>>>()
    );
};
