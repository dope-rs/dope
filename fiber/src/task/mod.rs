mod completion;
mod domain;
mod error;
mod group;
pub mod local;
mod scheduler;
pub mod sleep;
pub mod storage;

use std::task;

pub use completion::{once::Once, wait::Wait};
pub(crate) use domain::Domain;
pub use error::Error;
pub use group::{Group, admission::failures::GroupAdmissionError};
pub use scheduler::Scheduler;

use crate::{
    abi::{self, batch},
    context,
};

/// One-poll acquisition of ready slots dedicated to a batch.
#[must_use = "a batch domain is not acquired unless this fiber is driven"]
pub struct AcquireBatch<const N: usize> {
    _private: (),
}

impl<const N: usize> AcquireBatch<N> {
    pub(crate) const fn new() -> Self {
        Self { _private: () }
    }
}

impl<'d, const N: usize> abi::Fiber<'d> for AcquireBatch<N> {
    type Output = Result<batch::Domain<'d, N>, Error>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (_, context) = call.into_parts();
        task::Poll::Ready(batch::Domain::try_new(context.as_ref().root_waker()))
    }
}
