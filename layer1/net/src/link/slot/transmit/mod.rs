use dope_core::driver::result;

use crate::{link::event, wire::send::reclaim};

pub mod direct;
mod finalize;
mod graceful;
mod invalid;
pub mod outcome;
mod sender;
mod sending;

pub use finalize::Finalize;
pub use invalid::Invalid;
pub use sender::Sender;
pub use sending::Sending;

/// Immediate outcome of one egress submission attempt.
#[must_use]
pub enum Attempt {
    Runnable(reclaim::Advance),
    Waiting,
    Quiescent(reclaim::Advance),
    Rejected(event::SendFailure<result::SubmitError>, reclaim::Advance),
    Invalid(Invalid),
}
