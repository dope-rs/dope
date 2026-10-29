use dope_core::{
    driver,
    driver::{
        life::{self, recovery},
        retained,
        schedule::{clock, state},
        settings,
    },
};
use dope_test::scope::{Driver, Error};

pub mod profile;

use std::time;

use settings::demands::slots;
use state::scope;

pub struct Runtime {
    pub config: settings::Config,
}

impl Runtime {
    pub fn with_scope(
        self,
        run: impl for<'a, 'd> FnOnce(&mut life::Scope<'a, 'd>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner, ()>>),
    ) {
        Driver::new(self.config).with_scope(run, policy, finish)
    }

    pub fn with_driver(
        self,
        run: impl for<'a, 'd> FnOnce(driver::Context<'a, 'd>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner, ()>>),
    ) {
        Driver::new(self.config).with_driver(run, policy, finish)
    }

    pub fn with_turn(
        self,
        run: impl for<'turn, 'scope, 'd> FnOnce(
            &'turn mut scope::Guard<'scope, 'd>,
            retained::Context<'turn, 'd, 'd>,
        ),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner, ()>>),
    ) {
        self.with_scope(
            |scope| {
                scope.with_driver(|ctx, mut sched| {
                    let mut active = sched.begin_at(clock::Sample::new(time::Instant::now()));
                    let owner = retained::raw::Owner::from_driver(ctx.driver_ref());
                    run(&mut active, retained::Context::new(ctx, owner));
                });
            },
            policy,
            finish,
        )
    }

    pub fn throughput() -> Self {
        Self {
            config: settings::Config::for_profile::<profile::Throughput>(),
        }
    }

    pub fn for_profile<P: settings::Profile>() -> Self {
        let config = settings::Config::for_profile::<P>();
        Self { config }
    }

    pub fn quic<const SLOTS: u16, const BYTES: u32>() -> Self {
        let receive = settings::Datagram::fixed::<SLOTS, BYTES>();
        let config = settings::Config::new(
            settings::QueueLayout::fixed::<256, 1024>(),
            settings::Schedule::DEFAULT,
            settings::FileSlots::fixed::<0, 16>(),
            receive.receive(),
            settings::Domain::fixed::<2>(),
            settings::Progress::Prompt,
        );
        Self { config }
    }

    pub fn timers(mut self, cap: slots::Slots) -> Self {
        use settings::demands::Schedule;
        let schedule = settings::Schedule::fixed::<0, 0>()
            .resolve(Schedule::new(self.config.schedule().ready(), cap));
        self.config = self.config.set_schedule(schedule);
        self
    }

    pub const fn config(&self) -> settings::Config {
        self.config
    }

    pub fn queue_layout(mut self, queues: settings::QueueLayout) -> Self {
        self.config = self.config.with_queues(queues);
        self
    }

    pub fn file_slots(mut self, slots: settings::FileSlots) -> Self {
        self.config = self.config.with_files(slots);
        self
    }
}

pub mod record;
pub mod thread;

pub mod route;
