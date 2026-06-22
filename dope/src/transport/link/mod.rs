pub(super) mod core;
pub mod egress;
pub(super) mod pool;
pub(super) mod slot;

pub use core::{Core, Establish, Outbound};

pub use pool::{ConnectStep, DispatchRecv, Pool, SendOutcome, SocketStep};
pub use slot::{DeferredEgress, PEND_CLOSE, PEND_EGRESS, PEND_SHUTDOWN, PendingFlags, Slot};
