use dope::{
    core::{
        driver::{self, lifecycle, retained, route, schedule, settings},
        io::fs,
    },
    manifold::file,
    net::wire,
    runtime::executor::{self, session},
};

/// Active test turn that keeps reactor authority on the unique controller.
#[doc(hidden)]
pub struct RetainedTurn<'turn, 'scope, 'd> {
    turn: &'turn mut schedule::ActiveTurn<'scope, 'd>,
}

impl<'turn, 'scope, 'd> RetainedTurn<'turn, 'scope, 'd> {
    pub fn reborrow(&self) -> schedule::Turn<'_, 'd> {
        self.turn.turn()
    }

    pub fn application(&self) -> schedule::Application<'_, 'd> {
        self.turn.turn().application()
    }

    pub fn drain_ready(
        &self,
        driver: driver::Reference<'d>,
        limit: usize,
        activate: impl FnMut(route::Token),
    ) -> usize {
        self.turn.turn().drain_ready(driver, limit, activate)
    }

    pub fn reactor(&mut self) -> schedule::Reactor<'_, 'd> {
        self.turn.reactor()
    }
}
use dope_fiber::net::server;

use crate::checks::Outcome as _;

pub struct Runtime {
    config: settings::Config,
}

impl Runtime {
    pub fn throughput() -> Self {
        use dope::manifold::timing;
        Self::for_profile::<timing::Throughput>()
    }

    pub fn for_profile<P: settings::Profile>() -> Self {
        let config = settings::Config::for_profile::<P>().or_abort("build test driver profile");
        Self { config }
    }

    pub fn quic(buf_entries: u32, buf_len: u32) -> Self {
        let config =
            settings::Config::for_quic_udp(buf_entries, buf_len).or_abort("build QUIC test driver");
        Self { config }
    }

    pub fn timer_cache_limit(mut self, limit: settings::ScheduleCapacity) -> Self {
        let scheduler = self.config.scheduler().with_timer_cache_limit(limit);
        self.config = self.config.with_scheduler(scheduler);
        self
    }

    pub const fn config(&self) -> settings::Config {
        self.config
    }

    pub fn queue_layout(mut self, queues: settings::QueueLayout) -> Self {
        self.config = self.config.with_queue_layout(queues);
        self
    }

    pub fn file_slots(mut self, slots: settings::FileSlots) -> Self {
        self.config = self.config.with_file_slots(slots);
        self
    }

    pub fn executor(self) -> executor::Executor<()> {
        executor::Executor::new(self.config).or_abort("build test executor")
    }

    pub fn with_session<R>(
        self,
        f: impl for<'scope, 'd> FnOnce(session::Session<'scope, 'd>) -> R,
    ) -> R {
        self.executor().enter(f)
    }

    /// Runs a callback inside one generative driver domain.
    pub fn with_driver<R>(self, f: impl for<'a, 'd> FnOnce(driver::Context<'a, 'd>) -> R) -> R {
        use std::pin::pin;

        let driver = driver::Driver::new(self.config).or_abort("build test driver");
        let mut driver = pin!(driver);
        super::Scope::new(driver.as_mut()).enter(|mut scope| f(scope.context()))
    }

    /// Runs a raw-backend test with the exact driver-scope authority.
    #[doc(hidden)]
    pub fn with_retained_turn<R>(
        self,
        f: impl for<'turn, 'scope, 'd> FnOnce(
            RetainedTurn<'turn, 'scope, 'd>,
            retained::Context<'turn, 'd, 'd>,
        ) -> R,
    ) -> R {
        use std::pin::pin;

        let driver = driver::Driver::new(self.config).or_abort("build test driver");
        let mut driver = pin!(driver);
        super::Scope::new(driver.as_mut()).enter(|mut scope| {
            scope.with_turn(|_, context, mut controller| {
                let mut active = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
                f(
                    RetainedTurn { turn: &mut active },
                    super::Scope::retained_context(context),
                )
            })
        })
    }

    pub fn with_driver_scope<R>(self, f: impl for<'d> FnOnce(&mut lifecycle::Scope<'d>) -> R) -> R {
        use std::pin::pin;

        let driver = driver::Driver::new(self.config).or_abort("build scoped test driver");
        let mut driver = pin!(driver);
        super::Scope::new(driver.as_mut()).enter(|mut scope| f(&mut scope))
    }

    pub fn files<const ID: u8, const N: usize, F>(
        self,
    ) -> executor::Executor<file::FilesFactory<ID, N, F>>
    where
        F: fs::Mode + 'static,
    {
        use dope::manifold::file::Files;
        self.executor().with_factory(Files::<ID, N, F>::factory())
    }

    pub fn tcp_listener<P: settings::Profile>(
        max_connections: usize,
        tweak: impl FnOnce(settings::Config) -> settings::Config,
    ) -> executor::Executor<server::ListenerPortFactory<wire::Identity>> {
        use server::ListenerPort;
        let config = settings::Config::for_tcp_profile::<P>(max_connections)
            .or_abort("build TCP listener test profile");
        let config = tweak(config);
        let executor = executor::Executor::new(config).or_abort("build TCP listener test executor");
        let factory = ListenerPort::<wire::Identity>::factory(max_connections)
            .or_abort("build TCP listener test factory");
        executor.with_factory(factory)
    }
}

pub struct Tokens {
    values: Vec<(u8, u8, route::SlotIndex, Option<route::Epoch>)>,
}

impl Tokens {
    pub fn at(idx: u16) -> (u8, u8, route::SlotIndex, Option<route::Epoch>) {
        (
            0,
            0,
            route::SlotIndex::from(idx),
            Some(route::Epoch::INITIAL),
        )
    }

    pub fn target<'d>(
        driver: driver::Reference<'d>,
        idx: u16,
    ) -> route::Operation<'d, route::KeyTag<0>> {
        route::Space::for_driver(driver)
            .bind(route::SlotIndex::from(idx), route::Epoch::INITIAL)
            .dispatch()
    }

    pub fn parts(token: route::Token) -> (u8, u8, route::SlotIndex, Option<route::Epoch>) {
        (token.route(), token.kind(), token.slot(), token.epoch())
    }

    pub fn drain<'scope, 'd: 'scope, S>(session: &mut session::Session<'scope, 'd, S>) -> Self {
        Self {
            values: super::Scope::drain_ready(session)
                .into_iter()
                .map(Self::parts)
                .collect(),
        }
    }

    pub fn into_vec(self) -> Vec<(u8, u8, route::SlotIndex, Option<route::Epoch>)> {
        self.values
    }
}
