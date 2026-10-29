pub mod body;
mod commit;
mod context;
mod draft;
pub mod error;
mod outcome;
mod prefix;

pub use commit::Commit;
pub use context::Context;
use dope_net::link::egress::reserve::error::Full;
pub use draft::Draft;
pub use outcome::Outcome;
pub use prefix::Prefix;

/// Retryable cause whose exact wake authority is already registered.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Deferred {
    Turn,
    Capacity,
    OrderedDrain,
}

/// Permanent final-link rejection of a valid fallible write plan.
#[derive(Debug)]
pub enum Reject {
    Geometry(Full),
    Header,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum End {
    Open,
    Shutdown,
    Close,
}
