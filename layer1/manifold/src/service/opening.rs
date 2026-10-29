use dope_core::driver::{route::Identity, schedule::clock};
use dope_net::link::pool::change::opening;

use crate::client::attempt;

pub(crate) enum Opening<'d, E, ID: Identity> {
    Submitted,
    Deferred {
        attempt: attempt::Id<'d, ID>,
        cause: opening::Deferred,
    },
    Failed {
        attempt: attempt::Id<'d, ID>,
        error: opening::Failure<E>,
    },
    Backoff(clock::Deadline<'d>),
    Idle,
}
