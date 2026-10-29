use std::time;

use dope_core::driver::{route::Identity, schedule::clock};
use dope_net::{link::egress::data, wire};

use crate::network::listener::conn;

#[derive(Clone, Copy, Eq, PartialEq)]
#[must_use]
pub enum Arm<'d> {
    Armed,
    Disabled,
    Sealed,
    Overflow(Overflow<'d>),
}

/// Exact application-deadline overflow before any schedule mutation.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Overflow<'d> {
    pub now: clock::Moment<'d>,
    pub window: time::Duration,
}

const _: () = {
    assert!(
        std::mem::size_of::<Overflow<'static>>()
            == std::mem::size_of::<(clock::Moment<'static>, time::Duration)>()
    );
    assert!(!std::mem::needs_drop::<Overflow<'static>>());
};

/// Callback-scoped application deadline control for one connection generation.
pub struct Deadline<'e, 'turn, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B>
where
    B: data::raw::Payload<'d>,
{
    connection: &'turn mut conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>,
}

impl<'e, 'turn, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B>
    Deadline<'e, 'turn, 'a, 'p, 's, 'w, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    pub(super) const fn new(
        ctx: &'turn mut conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>,
    ) -> Self {
        Self { connection: ctx }
    }

    pub fn try_ensure(&mut self, now: clock::Moment<'d>) -> Arm<'d> {
        self.connection.slot.try_ensure(now)
    }
    pub fn try_arm(&mut self, at: clock::Deadline<'d>) -> Arm<'d> {
        self.connection.slot.try_arm(at)
    }
    pub fn cancel(&mut self) {
        self.connection.slot.cancel_app();
    }
}
