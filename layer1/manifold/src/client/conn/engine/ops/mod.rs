//! Connection engine state transitions.

pub(in crate::client) mod close;
pub(in crate::client) mod connect;
pub(in crate::client) mod dial;
mod fail;
pub(super) mod opens;
mod phase;
pub(in crate::client) mod retire;
