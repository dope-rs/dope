use std::mem;

use dope_net::link::{event, slot::transmit::outcome};

use crate::network::listener::writer::memory::access;

pub(in crate::network::listener::writer) struct Step<'a>(&'a mut access::Progress);

impl<'a> Step<'a> {
    pub(in crate::network::listener::writer) const fn new(
        progress: &'a mut access::Progress,
    ) -> Self {
        Self(progress)
    }

    #[must_use = "a direct-send step must apply its terminal action"]
    pub(in crate::network::listener::writer) fn finish(
        self,
        result: outcome::Handoff,
    ) -> outcome::Progress {
        let progress = result.progress();
        let arm = match progress {
            outcome::Progress::Submitted => access::Arm::Armed,
            outcome::Progress::Idle => access::Arm::Idle,
            outcome::Progress::Rejected(error) => {
                self.0.reject(event::SendFailure::Direct(error));
                return progress;
            }
        };
        self.0.record(result.consumed(), arm);
        progress
    }
}

const _: () = assert!(mem::size_of::<Step<'static>>() == mem::size_of::<usize>());
