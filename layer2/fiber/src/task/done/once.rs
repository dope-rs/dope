use ::core::task;

use crate::{
    abi,
    context::{self, Context},
    runtime::executor::root,
};

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

impl<'d, F> root::Root<'d> for Once<F>
where
    F: abi::Fiber<'d>,
{
    type Output = F::Output;

    fn poll(root: root::Context<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, wake, work, driver) = root.into_parts();
        Context::with_target(wake, work, driver, |mut context| {
            // SAFETY: the runtime owns this pinned root and exits its polling
            // loop on Ready; it cannot issue another Root::Context for this Once.
            unsafe { context::raw::Live::new(root.project().fiber).poll(context.as_mut()) }
        })
    }
}
