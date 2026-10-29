use dope_core::driver::{retained, route::Identity};
use dope_net::{
    link::{
        egress::{self, data},
        pool::pending,
        slot::{transmit::outcome, types},
    },
    wire::{self, send},
};
use send::reclaim;

use crate::network::listener::{conn, writer::memory::access};

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

pub(in crate::network::listener) trait SlotFlow<'d, ID: Identity> {
    fn flow(&self, pending: egress::Pending) -> Flow;
    fn after_send(&mut self, pending: egress::Pending) -> Handoff;
    fn advance_send(
        &mut self,
        slots: &crate::Flights<'d, ID>,
        pending: pending::Handle<'_, 'd, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B: data::raw::Payload<'d>>
    SlotFlow<'d, ID> for types::Connection<'w, 'd, ID, W, conn::State<'p, 's, 'd, ID, C, B>>
{
    fn flow(&self, pending: egress::Pending) -> Flow {
        use access::Motion;

        if self.send_status().is_inflight() {
            return Flow::Inflight;
        }
        let held = self.send_status().retention();
        match self.state.send.motion() {
            Motion::Empty => {}
            Motion::Consumed => return Flow::Plain,
            Motion::Waiting if held == outcome::Retention::Clear => return Flow::Stalled,
            Motion::Ready | Motion::Waiting => {
                let rule =
                    <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::direct();
                return match (rule, held) {
                    (reclaim::Direct::Independent, outcome::Retention::Clear) => Flow::Stalled,
                    (reclaim::Direct::Retained, _) | (_, outcome::Retention::Held) => Flow::Plain,
                };
            }
        }
        if held == outcome::Retention::Held {
            Flow::Held
        } else if matches!(pending, egress::Pending::Present) || self.send_status().is_closing() {
            Flow::Deferred
        } else {
            Flow::Clear
        }
    }

    fn after_send(&mut self, pending: egress::Pending) -> Handoff {
        match self.flow(pending) {
            Flow::Inflight => Handoff::Armed,
            Flow::Plain | Flow::Deferred => Handoff::Restage,
            Flow::Clear | Flow::Stalled | Flow::Held => Handoff::Clear,
        }
    }

    fn advance_send(
        &mut self,
        slots: &crate::Flights<'d, ID>,
        pending: pending::Handle<'_, 'd, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let (state, mut send) = self.split_send();
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
                (unsafe { send.submit_plain(slots, driver, input) }, step)
            }
            Ok(access::Prepared::Many(input, step)) => {
                // SAFETY: prepare derives the message, iovecs, header, and payload
                // from this connection's pinned Flight. Its exact lease and source
                // remain retained until completion or quiescence permits retirement.
                (unsafe { send.submit_many(input, slots, driver) }, step)
            }
        };
        match step.finish(result) {
            access::Finish::Ready => {}
            access::Finish::Rejected => send.discard(),
        }
        pending.wait_send(self.send_status().is_waiting());
    }
}
