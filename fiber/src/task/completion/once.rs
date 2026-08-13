use ::core::{pin, task};
use dope::runtime::executor;

use crate::{abi, context};

#[pin_project::pin_project]
pub struct Once<F> {
    #[pin]
    fiber: F,
}

impl<F> Once<F> {
    pub const fn new(fiber: F) -> Self {
        Self { fiber }
    }
}

impl<'d, F> executor::Root<'d> for Once<F>
where
    F: abi::Fiber<'d>,
{
    type Output = F::Output;

    fn poll(root: executor::RootContext<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, wake, work, driver) = root.into_parts();
        let mut context = pin::pin!(context::Context::from_target(wake, work, driver));
        context
            .as_mut()
            .try_poll(root.project().fiber)
            .unwrap_or(task::Poll::Pending)
    }
}
