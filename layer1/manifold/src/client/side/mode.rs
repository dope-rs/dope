use dope_core::{
    driver::{route::Identity, schedule::clock},
    io::socket,
};
use dope_net::link::oneshot;

use crate::client::{
    conn::engine::Attempts,
    side::{self, registry},
};

/// Side transport and its bounded retry schedule share the connector's alarm.
pub struct Transport<'d, T> {
    pub(in crate::client) io: T,
    pub(in crate::client) retry: Option<clock::Deadline<'d>>,
    pub(in crate::client) attempts: Attempts,
}

impl<T> Transport<'_, T> {
    pub(in crate::client) const fn new(io: T) -> Self {
        Self {
            io,
            retry: None,
            attempts: Attempts::INITIAL,
        }
    }
}

pub trait Mode<'d, ID: Identity> {
    type Transport<P: socket::Prepared>;
}

impl<'d, ID: Identity> Mode<'d, ID> for side::Disabled {
    type Transport<P: socket::Prepared> = ();
}

impl<'d, ID: Identity + 'd, I> Mode<'d, ID> for side::Enabled<&registry::Registry<'d, I, ID>>
where
    I: registry::Intent<'d, ID>,
{
    type Transport<P: socket::Prepared> =
        Transport<'d, oneshot::Pool<'d, ID, side::Ticket<'d, ID>, P, I::Payload>>;
}
