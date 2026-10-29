use std::mem;

use dope_net::link::{event, slot::transmit::outcome};

use crate::network::listener::writer::memory::access;

pub(in crate::network::listener::writer) struct Step<'a>(&'a mut access::Progress);

#[must_use = "a direct-send step must apply its terminal action"]
pub(in crate::network::listener::writer) enum Finish {
    Ready,
    Rejected,
}

impl<'a> Step<'a> {
    pub(in crate::network::listener::writer) const fn new(
        progress: &'a mut access::Progress,
    ) -> Self {
        Self(progress)
    }

    pub(in crate::network::listener::writer) fn finish(self, result: outcome::Handoff) -> Finish {
        let arm = match result.progress() {
            outcome::Progress::Submitted => access::Arm::Armed,
            outcome::Progress::Idle => access::Arm::Idle,
            outcome::Progress::Rejected(error) => {
                self.0.reject(event::SendFailure::Direct(error));
                return Finish::Rejected;
            }
        };
        self.0.record(result.consumed(), arm);
        Finish::Ready
    }
}

const _: () = assert!(mem::size_of::<Step<'static>>() == mem::size_of::<usize>());
