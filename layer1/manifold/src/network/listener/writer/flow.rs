use dope_core::driver::route::Identity;
use dope_net::{
    link::{
        egress::{self, data},
        slot::{transmit::outcome, types},
    },
    wire::{self, send},
};
use send::reclaim;

use crate::network::listener::writer::{deadline::Slot, memory::access, state};

pub(in crate::network::listener) enum Flow {
    Clear,
    Done,
    Inflight,
    Plain,
    Stalled,
    Held,
    Deferred,
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
        Motion::Done => return Flow::Done,
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
}
