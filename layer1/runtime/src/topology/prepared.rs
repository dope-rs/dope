use std::convert;

use dope_core::driver::{life::recovery, settings, storage};

use crate::{
    executor::{self, session},
    shutdown,
    topology::{self, ops, recipe},
};

/// Fully validated, proof-carrying startup input for one worker driver.
///
/// Retains the installer with its claims until the exact domain is entered.
pub struct Prepared<S, I> {
    domain: topology::Domain<S>,
    installer: I,
}

impl<S, I> Prepared<S, I> {
    pub(super) const fn new(layout: settings::Config, storage: S, setup: I) -> Self {
        Self {
            domain: topology::Domain::new(layout, storage),
            installer: setup,
        }
    }

    pub const fn layout(&self) -> settings::Config {
        self.domain.layout()
    }
}

impl<S, I> Prepared<S, I>
where
    S: storage::Direct,
{
    pub fn enter_local<O, R, Done>(
        self,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, convert::Infallible, (I, O), R>>,
        ) -> Done,
    ) -> Done
    where
        O: for<'scope, 'life, 'p, 'd> ops::Operation<
                'd,
                topology::Worker<'scope, 'life, 'p, 'd, S, I, ()>,
                Output = R,
            >,
    {
        self.domain
            .enter_local((self.installer, op), report, finish)
    }

    pub fn with_local<F, R, Done>(
        self,
        op: F,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, convert::Infallible, (I, F), R>>,
        ) -> Done,
    ) -> Done
    where
        F: for<'scope, 'life, 'p, 'd> FnOnce(
            topology::Worker<'scope, 'life, 'p, 'd, S, I, ()>,
        ) -> R,
    {
        self.domain
            .enter_local((self.installer, op), report, finish)
    }

    pub fn enter<C, O, R, Done>(
        self,
        context: C,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, convert::Infallible, (I, O), R, C::Shutdown>>,
        ) -> Done,
    ) -> Done
    where
        C: executor::Build,
        O: for<'scope, 'life, 'p, 'd> ops::Operation<
                'd,
                topology::Worker<'scope, 'life, 'p, 'd, S, I, C::Shutdown>,
                Output = R,
            >,
    {
        self.domain
            .enter(context, (self.installer, op), report, finish)
    }

    pub fn enter_with<C, F, R, Done>(
        self,
        context: C,
        op: F,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, convert::Infallible, (I, F), R, C::Shutdown>>,
        ) -> Done,
    ) -> Done
    where
        C: executor::Build,
        F: for<'scope, 'life, 'p, 'd> FnOnce(
            topology::Worker<'scope, 'life, 'p, 'd, S, I, C::Shutdown>,
        ) -> R,
    {
        self.domain
            .enter(context, (self.installer, op), report, finish)
    }

    /// Installs and runs one application, retaining every failure for its handler.
    pub fn run<'report, C, F>(
        self,
        context: C,
        complete: &'report mut session::Completion,
        report: impl recovery::Observe,
        failed: impl for<'owner> FnOnce(
            topology::RunError<
                'owner,
                convert::Infallible,
                I::Error,
                (I, ops::Run<'report>),
                shutdown::Requested<C::Shutdown>,
                C::Shutdown,
            >,
        ) -> Result<shutdown::Requested<C::Shutdown>, F>,
    ) -> Result<shutdown::Requested<C::Shutdown>, F>
    where
        C: executor::Build,
        I: recipe::Execute<S>,
    {
        self.enter(context, ops::Run(complete), report, |result| {
            let error = match result {
                Ok(Ok(done)) => return Ok(done),
                Ok(Err(error)) => topology::RunError::Failure(error),
                Err(error) => topology::RunError::from(error),
            };
            failed(error)
        })
    }
}

impl<S, I> Prepared<S, I>
where
    S: storage::Build,
{
    pub fn try_local<O, R, Done>(
        self,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, S::Error, (I, O), R>>,
        ) -> Done,
    ) -> Done
    where
        O: for<'scope, 'life, 'p, 'd> ops::Operation<
                'd,
                topology::Worker<'scope, 'life, 'p, 'd, S, I, ()>,
                Output = R,
            >,
    {
        self.domain.try_local((self.installer, op), report, finish)
    }

    pub fn try_scoped<F, R, Done>(
        self,
        op: F,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, S::Error, (I, F), R>>,
        ) -> Done,
    ) -> Done
    where
        F: for<'scope, 'life, 'p, 'd> FnOnce(
            topology::Worker<'scope, 'life, 'p, 'd, S, I, ()>,
        ) -> R,
    {
        self.domain.try_local((self.installer, op), report, finish)
    }

    pub fn try_enter<C, O, R, Done>(
        self,
        context: C,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, S::Error, (I, O), R, C::Shutdown>>,
        ) -> Done,
    ) -> Done
    where
        C: executor::Build,
        O: for<'scope, 'life, 'p, 'd> ops::Operation<
                'd,
                topology::Worker<'scope, 'life, 'p, 'd, S, I, C::Shutdown>,
                Output = R,
            >,
    {
        self.domain
            .try_enter(context, (self.installer, op), report, finish)
    }

    pub fn try_with<C, F, R, Done>(
        self,
        context: C,
        op: F,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, S::Error, (I, F), R, C::Shutdown>>,
        ) -> Done,
    ) -> Done
    where
        C: executor::Build,
        F: for<'scope, 'life, 'p, 'd> FnOnce(
            topology::Worker<'scope, 'life, 'p, 'd, S, I, C::Shutdown>,
        ) -> R,
    {
        self.domain
            .try_enter(context, (self.installer, op), report, finish)
    }
}

impl<S, I> Prepared<S, I>
where
    S: storage::Create,
{
    pub fn try_create<F, R, Done>(
        self,
        op: F,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, S::Error, (I, F), R>>,
        ) -> Done,
    ) -> Done
    where
        F: for<'scope, 'life, 'p, 'd> FnOnce(
            topology::Worker<'scope, 'life, 'p, 'd, S, I, ()>,
        ) -> R,
    {
        self.domain.try_create((self.installer, op), report, finish)
    }

    pub fn try_scope<O, R, Done>(
        self,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<R, topology::EnterError<'owner, S::Error, (I, O), R>>,
        ) -> Done,
    ) -> Done
    where
        O: for<'scope, 'life, 'p, 'd> ops::Operation<
                'd,
                topology::Worker<'scope, 'life, 'p, 'd, S, I, ()>,
                Output = R,
            >,
    {
        self.domain.try_create((self.installer, op), report, finish)
    }
}
