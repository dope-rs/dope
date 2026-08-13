//! Connection engine state transitions.

pub(in crate::connector) mod close;
pub(super) mod connect;
pub(in crate::connector) mod dial;
pub(in crate::connector) mod retire;
pub(in crate::connector) mod shutdown;
