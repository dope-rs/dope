use std::mem;

use dope_core::{
    driver::{route::Identity, schedule::clock},
    io::socket,
};
use o3::base::time::Positive;

use crate::client::{conn::engine::liveness, deadline};

#[derive(Clone, Copy)]
pub(in crate::client) enum Deadline {
    None,
    Ensure,
    Progress,
    Cancel,
}

impl Deadline {
    pub(super) fn apply<'d, ID: Identity, P, X, C>(
        self,
        source: liveness::Change<'_, 'd, ID, P, X, C>,
        now: clock::Moment<'d>,
        window: Positive,
    ) -> C
    where
        P: socket::Prepared,
        X: liveness::Schedule<'d, ID, P>,
    {
        match self {
            Self::None => source.into_source(),
            Self::Ensure => source.ensure_after(deadline::Fixed::Send, now, window),
            Self::Progress => source.arm_after(deadline::Fixed::Send, now, window),
            Self::Cancel => source.cancel(deadline::Kind::Send),
        }
    }
}

const _: () = assert!(mem::size_of::<Deadline>() == 1);
