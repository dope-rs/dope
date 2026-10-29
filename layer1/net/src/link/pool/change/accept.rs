use dope_core::{
    driver::route::Identity,
    io::{event::net::tuning, socket::pending},
};

use crate::link::pool;

/// Result of installing and tuning one accepted socket.
pub enum Outcome<'d, ID: Identity, R> {
    Ready(pool::Key<'d, ID>),
    Pending,
    Failed(pool::Key<'d, ID>, pending::Cause),
    Unavailable,
    IdentityExhausted,
    Rejected(R),
}

/// Result of consuming one accepted-socket tuning completion.
pub enum Completion<'d, ID: Identity> {
    Ready(pool::Key<'d, ID>),
    Failed(pool::Key<'d, ID>, dope_core::Errno),
    Cancelled(pool::Key<'d, ID>),
    Stale(tuning::Completion<'d>),
}
