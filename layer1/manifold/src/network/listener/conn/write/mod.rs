pub mod body;
mod context;
mod direct;
pub mod error;
mod output;

pub use context::Context;
use dope_net::link::egress::reserve::error::Full;
pub use output::Output;

pub const HEADER_CAPACITY: usize = crate::network::listener::writer::memory::WRITE_BUF_CAP;

/// Retryable cause whose exact wake authority is already registered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
