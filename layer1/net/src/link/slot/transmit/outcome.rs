use dope_core::driver::{result, route::Identity, schedule::ready};
use send::reclaim;

use crate::{
    link::{self, pool, slot::types},
    wire::{self, send},
};

#[must_use = "an armed send wait must reach its exact connection owner"]
pub struct Wait<'d, ID: Identity> {
    key: pool::Key<'d, ID>,
    target: ready::Target<'d>,
}

impl<'d, ID: Identity> Wait<'d, ID> {
    pub(in crate::link::slot::transmit) fn new<'w, W: wire::Spec>(
        slot: &types::Control<'w, 'd, ID, W>,
    ) -> Self
    where
        ID: 'w,
    {
        Self {
            key: slot.key(),
            target: slot.io().wake_target(),
        }
    }

    pub(in crate::link) fn matches(
        &self,
        key: pool::Key<'d, ID>,
        target: ready::Target<'d>,
    ) -> bool {
        self.key == key && self.target == target
    }

    /// Consumes the exact wait evidence by scheduling its registered owner.
    pub fn reschedule(self) {
        self.target.wake();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    Clear,
    Held,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Progress {
    Submitted,
    Idle,
    Rejected(result::SubmitError),
}

/// Exact result of one direct handoff. This is stack-only evidence; connection
/// state retains the owner/cursor while the caller decides retry or close.
#[derive(Clone, Copy)]
pub struct Handoff {
    consumed: link::Consumed,
    progress: Progress,
}

impl Handoff {
    pub const fn blocked() -> Self {
        Self {
            consumed: link::Consumed::ZERO,
            progress: Progress::Idle,
        }
    }

    pub const fn consumed(self) -> link::Consumed {
        self.consumed
    }

    pub const fn progress(self) -> Progress {
        self.progress
    }

    pub(in crate::link) fn from_attempt<P: reclaim::Policy>(
        attempt: send::Attempt<P>,
    ) -> Self {
        let (credit, state) = attempt.into_parts();
        let amount = P::immediate(credit);
        let kind = match state {
            send::State::Submitted => Progress::Submitted,
            send::State::Rejected(error) => Progress::Rejected(error),
            send::State::Idle => Progress::Idle,
        };
        Self {
            consumed: link::Consumed::proven(amount),
            progress: kind,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Handoff>() == std::mem::size_of::<usize>());
