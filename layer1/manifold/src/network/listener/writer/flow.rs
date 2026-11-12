use dope_core::driver::{self, route::Identity};
use dope_net::{
    link::{
        egress::{self, data},
        pool::pending,
        slot::{
            transmit::{direct::raw, outcome},
            types,
        },
    },
    wire::{self, send},
};
use send::reclaim;

use crate::network::listener::writer::{deadline::Slot, memory::access, state};

pub(in crate::network::listener) enum Flow {
    Clear,
    Inflight,
    Plain,
    Stalled,
    Held,
    Deferred,
}

pub(in crate::network::listener) enum Handoff {
    Clear,
    Armed,
    Restage,
}

pub(in crate::network::listener) fn flow<
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
>(
    control: &types::Control<'w, 'd, ID, W>,
    send: &state::Send<'p, 's, 'd, ID, B>,
    pending: egress::Pending,
) -> Flow {
    use access::Motion;

    if control.send_status().is_inflight() {
        return Flow::Inflight;
    }
    let held = control.send_status().retention();
    match send.motion() {
        Motion::Empty => {}
        Motion::Consumed => return Flow::Plain,
        Motion::Waiting if held == outcome::Retention::Clear => return Flow::Stalled,
        Motion::Ready | Motion::Waiting => {
            let rule =
                <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::direct(
                );
            return match (rule, held) {
                (reclaim::Direct::Independent, outcome::Retention::Clear) => Flow::Stalled,
                (reclaim::Direct::Retained, _) | (_, outcome::Retention::Held) => Flow::Plain,
            };
        }
    }
    if held == outcome::Retention::Held {
        Flow::Held
    } else if matches!(pending, egress::Pending::Present) || control.send_status().is_closing() {
        Flow::Deferred
    } else {
        Flow::Clear
    }
}

impl<'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B: data::raw::Payload<'d>>
    Slot<'_, 'p, 's, 'w, 'd, ID, W, C, B>
{
    pub(in crate::network::listener) fn flow(&self, pending: egress::Pending) -> Flow {
        flow(self.source, &self.state.send, pending)
    }

    pub(in crate::network::listener) fn after_send(&mut self, pending: egress::Pending) -> Handoff {
        match self.flow(pending) {
            Flow::Inflight => Handoff::Armed,
            Flow::Plain | Flow::Deferred => Handoff::Restage,
            Flow::Clear | Flow::Stalled | Flow::Held => Handoff::Clear,
        }
    }

    pub(in crate::network::listener) fn advance_send(
        &mut self,
        slots: &crate::Flights<'d, ID>,
        pending: pending::Handle<'_, 'd, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let state = &mut *self.state;
        let (result, step) = match state.send.prepare() {
            Err(error) => {
                state.send.invalidate(error);
                self.io().close();
                return;
            }
            Ok(access::Prepared::Idle) => return,
            Ok(access::Prepared::Plain(input, step)) => {
                // SAFETY: prepare borrows this connection's pinned Flight header.
                // Its direct state keeps the exact lease until send retirement;
                // completion or quiescence precedes releasing that owner.
                (
                    unsafe { raw::submit_plain(self.source, slots, driver, input) },
                    step,
                )
            }
            Ok(access::Prepared::Many(input, step)) => {
                // SAFETY: prepare derives the message, iovecs, header, and payload
                // from this connection's pinned Flight. Its exact lease and source
                // remain retained until completion or quiescence permits retirement.
                (
                    unsafe { raw::submit_many(self.source, input, slots, driver) },
                    step,
                )
            }
        };
        match step.finish(result) {
            access::Finish::Ready => {}
            access::Finish::Rejected => self.source.discard(),
        }
        pending.wait_send(self.send_status().is_waiting());
    }
}
