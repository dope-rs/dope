use std::{marker, mem, pin};

use dope_core::driver::{
    self,
    route::{Identity, KeyTag},
    schedule::clock::{self, watchdog},
    settings::{self, claim, demands, errors},
};
use dope_runtime::executor;
use o3::base::time::Positive;

use crate::{
    app::{creation, dispatch},
    timing::timer,
};

#[derive(Clone, Copy)]
pub struct Config {
    period: Positive,
    deadline: Positive,
}

/// Exact startup-only driver layout for one watchdog timer route.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Layout<ID: Identity>(marker::PhantomData<fn() -> ID>);

impl<ID: Identity> Default for Layout<ID> {
    fn default() -> Self {
        Self(marker::PhantomData)
    }
}

impl<ID: Identity> settings::Resource for Layout<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(
            claim::Claim::new(settings::Routes::singleton::<ID>())
                .schedule(demands::Schedule::fixed::<1, 1>()),
        )
    }
}

impl Config {
    pub fn new(period: Positive, deadline: Positive) -> Result<Self, crate::ConfigError> {
        if period >= deadline {
            return Err(crate::ConfigError::WatchdogOrder { period, deadline });
        }
        Ok(Self { period, deadline })
    }

    pub const fn period(self) -> Positive {
        self.period
    }
}

/// Worker-local death timer refreshed only by its dispatched timer route.
/// It stays armed through shutdown and is disarmed by exact-root finalization.
#[pin_project::pin_project]
pub struct Watchdog<'d, ID: Identity> {
    #[pin]
    route: timer::Timer<'d, ID>,
    kernel: watchdog::Timer<'d>,
    period: Positive,
    next: Option<clock::Deadline<'d>>,
}

impl<'d, ID: Identity> Watchdog<'d, ID> {
    pub fn new(
        config: Config,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Self, creation::Error> {
        let period = config.period();
        let deadline = config.deadline;
        let now = driver.driver_ref().clock().now();
        let next = now
            .try_add(period)
            .map_err(crate::ConfigError::WatchdogRange)
            .map_err(creation::Error::Config)?;
        let route = timer::Timer::new(driver)?;
        let kernel = watchdog::Timer::try_new(deadline, driver.driver_ref())
            .map_err(creation::Error::Watchdog)?;
        Ok(Self {
            route,
            kernel,
            period,
            next: Some(next),
        })
    }
}

impl<'d, ID: Identity> executor::raw::Finishes<'d, Self> for Watchdog<'d, ID> {
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        if let Err(cause) = owner.as_mut().project().kernel.try_disarm(proof) {
            proof.fail_disarm(cause);
        }
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<'d, ID: Identity> dispatch::raw::Install<'d> for Watchdog<'d, ID> {
    fn install(mut self: pin::Pin<&mut Self>) {
        let this = self.as_mut().project();
        if let Some(deadline) = *this.next {
            this.route.as_ref().arm(deadline);
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Watchdog<'static, KeyTag<0>>>()
            == mem::size_of::<(
                timer::Timer<'static, KeyTag<0>>,
                watchdog::Timer<'static>,
                Positive,
                Option<clock::Deadline<'static>>,
            )>()
    );
};

mod raw;
