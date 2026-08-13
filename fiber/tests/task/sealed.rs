use std::pin::Pin;

use dope::core::driver;

pub(crate) fn retained_context<'a, 'd>(
    context: driver::Context<'a, 'd>,
) -> driver::retained::Context<'a, 'd, 'd> {
    // SAFETY: the driver owns this timer for the complete generative scope.
    let timer = unsafe { Pin::new_unchecked(context.timer()) };
    // SAFETY: the timer remains pinned through the scope's final quiescence.
    let owner = unsafe { driver::retained::raw::Owner::new(timer) };
    driver::retained::Context::new(context, owner)
}
