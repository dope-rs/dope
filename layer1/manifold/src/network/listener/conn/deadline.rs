use dope_core::driver::{route::Identity, schedule::clock};
use dope_net::{link::egress::data, wire};

use crate::network::listener::conn;

pub use crate::network::listener::writer::deadline::Kind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Disabled,
    Sealed,
    Overflow(clock::Range),
}

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

    pub fn try_ensure(&mut self, now: clock::Moment<'d>) -> Result<(), Error> {
        self.connection.slot.try_ensure(now)
    }
    pub fn try_arm(&mut self, at: clock::Deadline<'d>) -> Result<(), Error> {
        self.connection.slot.try_arm(at)
    }
    pub fn cancel(&mut self) {
        self.connection.slot.cancel_app();
    }

    pub fn reject(self, cause: Error) {
        self.connection
            .slot
            .state
            .fail(conn::Failure::Deadline(cause));
        self.connection.slot.close_after();
    }
}
