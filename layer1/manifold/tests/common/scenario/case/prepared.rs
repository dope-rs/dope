use std::convert::Infallible;

use dope_core::driver::{life::recovery, route::Identity, settings};
use dope_manifold::{app::env, client::app::request};
use dope_runtime::{
    executor::session::Completion,
    topology::{self, error::Kind, ops},
};

use crate::common::scenario::{
    RuntimeCase,
    case::{Client, Connect, Failure},
};

/// The original client installation and its resolved driver layout.
pub struct Prepared<ID: Identity, const MAX: usize, E: env::Spec, F, Q = request::Enabled> {
    plan: topology::Prepared<(), Client<ID, MAX, E, F, Q>>,
}

impl<ID: Identity, const MAX: usize, E: env::Spec, F, Q> Prepared<ID, MAX, E, F, Q> {
    pub(super) fn new(plan: topology::Prepared<(), Client<ID, MAX, E, F, Q>>) -> Self {
        Self { plan }
    }

    pub const fn layout(&self) -> settings::Config {
        self.plan.layout()
    }

    pub fn with_run<'report, B, R, K, Done>(
        self,
        complete: &'report mut Completion,
        body: B,
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<
                Result<R, Failure<'report, K, R>>,
                Kind<'owner, Infallible, &'owner Result<R, Failure<'report, K, R>>>,
            >,
        ) -> Done,
    ) -> Done
    where
        B: FnOnce(&mut dyn RuntimeCase) -> R,
        Connect<'report, B>: for<'scope, 'life, 'p, 'd> ops::Operation<
                'd,
                topology::Worker<'scope, 'life, 'p, 'd, (), Client<ID, MAX, E, F, Q>, ()>,
                Output = Result<R, Failure<'report, K, R>>,
            >,
    {
        self.plan.enter_local(
            Connect {
                finish: complete,
                body,
            },
            policy,
            |outcome| match outcome {
                Ok(output) => finish(Ok(output)),
                Err(error) => finish(Err(error.kind())),
            },
        )
    }
}
