use std::fmt::Debug;

use dope_core::driver::route::Identity;
use dope_manifold::app::env;
use dope_runtime::{
    executor::session::Completion,
    topology::{self, ops},
};

use crate::common::scenario::{
    RuntimeCase,
    case::{Client, Connect, Failure},
};

/// The original client installation and its resolved driver layout.
pub struct Prepared<ID: Identity, const MAX: usize, E: env::Spec, F> {
    plan: topology::Prepared<(), Client<ID, MAX, E, F>>,
}

impl<ID: Identity, const MAX: usize, E: env::Spec, F> Prepared<ID, MAX, E, F> {
    pub(super) fn new(plan: topology::Prepared<(), Client<ID, MAX, E, F>>) -> Self {
        Self { plan }
    }

    pub fn with_worker<R: Debug, K: Debug>(
        self,
        run: impl for<'scope, 'life, 'p, 'd> FnOnce(
            topology::Worker<'scope, 'life, 'p, 'd, (), Client<ID, MAX, E, F>, ()>,
        ) -> Result<R, K>,
    ) -> R {
        super::checked(super::enter(self.plan, ID::ROUTE, run))
    }

    pub fn run<B, R: Debug, K: Debug>(self, body: B) -> R
    where
        B: FnOnce(&mut dyn RuntimeCase) -> R,
        for<'report, 'scope, 'life, 'p, 'd> Connect<'report, B>: ops::Operation<
                'd,
                topology::Worker<'scope, 'life, 'p, 'd, (), Client<ID, MAX, E, F>, ()>,
                Output = Result<R, Failure<'report, K, R>>,
            >,
    {
        super::checked(super::enter(
            self.plan,
            ID::ROUTE,
            Connect {
                finish: &mut Completion::new(),
                body,
            },
        ))
    }
}
