use dope_core::driver::{
    self,
    life::{self, creation, domain, recovery},
    settings,
};

use crate::scope::Error;

/// Runs an admitted driver configuration through its complete native lifecycle.
pub struct Driver(settings::Config);

impl Driver {
    pub const fn new(config: settings::Config) -> Self {
        Self(config)
    }

    pub fn with_scope<R, Done>(
        self,
        run: impl for<'a, 'd> FnOnce(&mut life::Scope<'a, 'd>) -> R,
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<R, Error<'owner, R>>) -> Done,
    ) -> Done {
        let mut attempt = creation::Attempt::new();
        let driver = match attempt.try_driver(self.0) {
            Ok(driver) => driver,
            Err(error) => return finish(Err(Error::Create(error))),
        };
        let mut teardown = domain::Teardown::default();
        finish(
            domain::Generation::new(driver)
                .start(&mut teardown)
                .enter_with((), |mut scope, _, _| run(&mut scope), policy)
                .map_err(Error::Close),
        )
    }

    pub fn with_driver<R, Done>(
        self,
        run: impl for<'a, 'd> FnOnce(driver::Context<'a, 'd>) -> R,
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<R, Error<'owner, R>>) -> Done,
    ) -> Done {
        self.with_scope(|scope| run(scope.context()), policy, finish)
    }
}
