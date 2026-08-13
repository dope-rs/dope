use std::io;

use o3::cell::{brand, region};

use crate::{
    backend::{self, fixed},
    driver::{
        self, flight,
        ops::{reactors, retirements},
        schedule,
        schedule::timer,
    },
};

mod domain;
mod finalize;
mod install;
mod sealed;

pub use domain::Domain;
pub(crate) use domain::Source;
pub use finalize::Finalize;
pub use install::Install;
pub(in crate::driver) use sealed::Lease;
pub mod quiesce;
#[doc(hidden)]
pub mod raw;
pub mod routing;

pub struct Scope<'d> {
    driver: driver::Reference<'d>,
    backend: &'d mut backend::Backend,
    flights: &'d mut flight::Arena,
    work: schedule::Work,
    region: region::Token<'d>,
    token: brand::Token<'d>,
    timer: &'d timer::Timer<'d>,
    _owner: quiesce::Lease,
}

impl<'d> Scope<'d> {
    pub(in crate::driver) fn new(
        driver: driver::Reference<'d>,
        backend: &'d mut backend::Backend,
        flights: &'d mut flight::Arena,
        region: region::Token<'d>,
        token: brand::Token<'d>,
        timer: &'d timer::Timer<'d>,
        owner: quiesce::Lease,
    ) -> Self {
        Self {
            driver,
            backend,
            flights,
            work: schedule::Work::new(),
            region,
            token,
            timer,
            _owner: owner,
        }
    }

    pub fn context(&mut self) -> driver::Context<'_, 'd> {
        driver::Context::new(
            self.driver,
            &mut *self.backend,
            &mut *self.flights,
            &mut self.region,
            self.timer,
        )
    }

    pub fn token(&mut self) -> &mut brand::Token<'d> {
        &mut self.token
    }

    /// Runs with the sole scheduler coordinator for this driver scope.
    ///
    /// ```compile_fail
    /// use dope_core::driver::{lifecycle::Scope, schedule::Controller};
    ///
    /// fn escape<'d>(scope: &mut Scope<'d>) -> Controller<'static, 'd> {
    ///     scope.with_turn(|_, _, turn| turn)
    /// }
    /// ```
    pub fn with_turn<R>(
        &mut self,
        run: impl for<'scope> FnOnce(
            &'scope mut brand::Token<'d>,
            driver::Context<'scope, 'd>,
            schedule::Controller<'scope, 'd>,
        ) -> R,
    ) -> R {
        let context = driver::Context::new(
            self.driver,
            &mut *self.backend,
            &mut *self.flights,
            &mut self.region,
            self.timer,
        );
        run(
            &mut self.token,
            context,
            schedule::Controller::new(self.driver, &mut self.work),
        )
    }

    pub fn driver_ref(&self) -> driver::Reference<'d> {
        self.driver
    }

    #[doc(hidden)]
    pub fn final_quiescence(
        &mut self,
    ) -> io::Result<(&mut brand::Token<'d>, quiesce::Final<'_, 'd>)> {
        let context = driver::Context::new(
            self.driver,
            &mut *self.backend,
            &mut *self.flights,
            &mut self.region,
            self.timer,
        );
        let finalization = quiesce::Final::new(context)?;
        Ok((&mut self.token, finalization))
    }

    #[doc(hidden)]
    pub fn reap_finalized(&mut self) -> io::Result<()> {
        let mut context = driver::Context::new(
            self.driver,
            &mut *self.backend,
            &mut *self.flights,
            &mut self.region,
            self.timer,
        );
        reactors::Returned::reclaim_all(&mut context);
        retirements::Reclaimer::<true>::new(&mut context).all();
        let (backend, drain) = context.backend_drain();
        fixed::Finalize::settle(backend, drain)
    }
}
