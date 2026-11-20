use dope_core::driver::route::Identity;
use dope_net::{
    link::{
        egress::{self, data},
        slot::{transmit::{direct::Status, outcome}, types},
    },
    wire::{self, send},
};
use send::reclaim;

use crate::network::listener::writer::{deadline::Slot, memory::access, state};

#[derive(Clone, Copy)]
pub(in crate::network::listener) enum Flow {
    Clear,
    Done,
    Inflight,
    Plain,
    Stalled,
    Held,
    Deferred,
}

impl Flow {
    pub(super) fn runnable(self, waiting: bool) -> bool {
        match self {
            Self::Done => true,
            Self::Plain | Self::Deferred => !waiting,
            _ => false,
        }
    }

    pub(super) fn observe<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w>(
        self,
        sending: Status<'_, 'w, 'd, ID, W>,
    ) -> Self {
        match self {
            Self::Clear | Self::Deferred if sending.retention() == outcome::Retention::Held => Self::Held,
            Self::Stalled if sending.retention() == outcome::Retention::Held => Self::Plain,
            other => other,
        }
    }

    pub(super) fn retry(self) -> Self {
        match self {
            Self::Stalled => Self::Plain,
            other => other,
        }
    }
}

pub(super) fn unheld<
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
    match send.motion() {
        Motion::Empty => {}
        Motion::Done => return Flow::Done,
        Motion::Consumed => return Flow::Plain,
        Motion::Waiting => return Flow::Stalled,
        Motion::Ready => {
            let rule =
                <<<W as wire::Spec>::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::direct(
                );
            return match rule {
                reclaim::Direct::Independent => Flow::Stalled,
                reclaim::Direct::Retained => Flow::Plain,
            };
        }
    }
    if matches!(pending, egress::Pending::Present) || control.send_status().is_closing() {
        Flow::Deferred
    } else {
        Flow::Clear
    }
}

impl<'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B: data::raw::Payload<'d>>
    Slot<'_, 'p, 's, 'w, 'd, ID, W, C, B>
{
    pub(in crate::network::listener) fn flow(&self, pending: egress::Pending) -> Flow {
        unheld(self.source, &self.state.send, pending).observe(self.source.send_status())
    }
}
