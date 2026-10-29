use std::convert;

use dope_core::driver::{
    life::{creation, domain, recovery},
    settings::{self, claim, errors},
    storage,
};

use crate::{executor, topology::EnterError};

/// Fully validated driver layout plus its generative storage plan.
pub struct Domain<S> {
    layout: settings::Config,
    storage: S,
}

impl<S> Domain<S> {
    pub(super) const fn new(layout: settings::Config, store: S) -> Self {
        Self {
            layout,
            storage: store,
        }
    }

    pub const fn layout(&self) -> settings::Config {
        self.layout
    }

    pub fn for_profile<P>(store: S) -> Result<Self, errors::Claim>
    where
        P: settings::Profile,
        S: storage::Build + settings::Resource,
    {
        let layout = claim::Needs::from_tree(&store)?.resolve::<P>()?;
        Ok(Self::new(layout, store))
    }
}

impl<S> Domain<S>
where
    S: storage::Direct,
{
    pub fn enter_local<O, Done>(
        self,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<O::Output, EnterError<'owner, convert::Infallible, O, O::Output>>,
        ) -> Done,
    ) -> Done
    where
        O: executor::Op<S, ()>,
    {
        self.enter((), op, report, finish)
    }

    pub fn enter<C, O, Done>(
        self,
        ctx: C,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<O::Output, EnterError<'owner, convert::Infallible, O, O::Output, C::Shutdown>>,
        ) -> Done,
    ) -> Done
    where
        C: executor::Build,
        O: executor::Op<S, C::Shutdown>,
    {
        let mut attempt = creation::Attempt::new();
        let exec = match ctx.try_build(&mut attempt, self.layout) {
            Ok(exec) => exec.with_plan(self.storage),
            Err(cause) => {
                return finish(Err(EnterError::Runtime(domain::Rejected {
                    cause,
                    operation: op,
                })));
            }
        };
        exec.run(|run| {
            finish(
                run.enter(op, report)
                    .map_err(|error| EnterError::from(error.map_storage(|never| match never {}))),
            )
        })
    }
}

impl<S> Domain<S>
where
    S: storage::Build,
{
    pub fn try_local<O, Done>(
        self,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<O::Output, EnterError<'owner, S::Error, O, O::Output>>,
        ) -> Done,
    ) -> Done
    where
        O: executor::Op<S, ()>,
    {
        self.try_enter((), op, report, finish)
    }

    pub fn try_enter<C, O, Done>(
        self,
        ctx: C,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<O::Output, EnterError<'owner, S::Error, O, O::Output, C::Shutdown>>,
        ) -> Done,
    ) -> Done
    where
        C: executor::Build,
        O: executor::Op<S, C::Shutdown>,
    {
        let mut attempt = creation::Attempt::new();
        let exec = match ctx.try_build(&mut attempt, self.layout) {
            Ok(exec) => exec.with_plan(self.storage),
            Err(cause) => {
                return finish(Err(EnterError::Runtime(domain::Rejected {
                    cause,
                    operation: op,
                })));
            }
        };
        exec.run(|run| finish(run.try_enter(op, report).map_err(EnterError::from)))
    }
}

impl<S> Domain<S>
where
    S: storage::Create,
{
    pub(super) fn try_create<O, Done>(
        self,
        op: O,
        report: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<O::Output, EnterError<'owner, S::Error, O, O::Output>>,
        ) -> Done,
    ) -> Done
    where
        O: executor::Op<S, ()>,
    {
        let mut attempt = creation::Attempt::new();
        let exec = match executor::Executor::new(&mut attempt, self.layout) {
            Ok(exec) => exec.with_plan(self.storage),
            Err(cause) => {
                return finish(Err(EnterError::Runtime(domain::Rejected {
                    cause,
                    operation: op,
                })));
            }
        };
        exec.run(|run| finish(run.try_scope(op, report).map_err(EnterError::from)))
    }
}
