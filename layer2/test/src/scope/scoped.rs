use dope_core::driver::storage;
use dope_runtime::executor::{self, session};

/// Adapts one closure to the executor's full storage and loan lifetime contract.
#[repr(transparent)]
pub struct Scoped<F>(F);

impl<F> Scoped<F> {
    pub const fn new<S: storage::Plan, Q, R>(run: F) -> Self
    where
        F: for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> R,
    {
        Self(run)
    }

    pub fn into_inner(self) -> F {
        self.0
    }
}

impl<S: storage::Plan, Q, F, R> executor::Op<S, Q> for Scoped<F>
where
    F: for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> R,
{
    type Output = R;
    fn run<'scope, 'life, 'p, 'd>(self, scope: session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> R
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: 'd,
    {
        (self.0)(scope)
    }
}
