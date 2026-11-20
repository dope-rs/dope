use std::{marker, net, time::Duration};

use dope_core::driver::{
    life::recovery,
    route::{Identity, KeyTag, table::conn::Capacity},
    settings::{self, claim, errors},
    storage,
};
use dope_manifold::{
    app::{Host, creation, env},
    client::{
        app,
        attempt::{self, queue},
        conn::engine,
    },
};
use dope_net::{
    Transport as _,
    link::{egress, pool::change},
    tcp,
};
use dope_runtime::{
    executor::{self, session, session::Completion},
    topology::error::Kind,
};
use dope_test::reclaim::Reports;
use o3::depot::{arena::slab, capacity};
use queue::mailbox;

use crate::common::scenario;

pub struct Attempts<ID: Identity>(capacity::Count, marker::PhantomData<fn() -> ID>);

impl<ID: Identity> Attempts<ID> {
    pub const fn new(capacity: capacity::Count) -> Self {
        Self(capacity, marker::PhantomData)
    }
}

#[derive(Debug)]
pub enum Failure {
    Attempt(slab::Vacancy),
    Engine(creation::Error),
}

impl<ID: Identity> storage::Plan for Attempts<ID> {
    type Output<'p, 'd>
        = queue::Source<'d, tcp::Tcp, ID>
    where
        Self: 'd,
        'd: 'p;
}

impl<ID: Identity> storage::Create for Attempts<ID> {
    type Error = creation::Error;

    fn try_build<'p, 'd>(
        self,
        mut context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        queue::Source::try_new(self.0, &mut context)
    }
}

/// One exact low-level attempt, used by tests that exercise the raw connector
/// application boundary rather than service discovery or redial policy.
pub struct Attempt {
    address: net::SocketAddr,
    stream: tcp::StreamConfig,
}

impl Attempt {
    pub const fn new(address: net::SocketAddr) -> Self {
        Self {
            address,
            stream: tcp::StreamConfig {
                recv_buffer_size: None,
                send_buffer_size: None,
                no_delay: None,
                keep_alive: None,
                keep_alive_idle: None,
                keep_alive_interval: None,
                keep_alive_retries: None,
            },
        }
    }

    pub const fn stream(mut self, stream: tcp::StreamConfig) -> Self {
        self.stream = stream;
        self
    }

    pub fn prepare<ID: Identity, A, E>(self, app: A) -> Prepared<ID, A, E>
    where
        E: env::Spec<Transport = tcp::Tcp>,
    {
        Prepared::<ID, A, E> {
            attempt: self,
            app,
            config: settings::Config::for_profile::<E::Driver>(),
            env: marker::PhantomData,
        }
    }
}

/// One attempt and its exact resolved driver layout, before execution.
pub struct Prepared<ID: Identity, A, E> {
    attempt: Attempt,
    app: A,
    config: settings::Config,
    env: marker::PhantomData<fn() -> (E, ID)>,
}

impl<ID: Identity, A, E> Prepared<ID, A, E> {
    pub fn run<B>(self, binding: B, body: impl FnOnce(&mut dyn scenario::RuntimeCase))
    where
        E: env::Spec<Transport = tcp::Tcp>,
        for<'d> E::Wire: 'd,
        for<'d> B: change::Prepare<'d, 'd, ID, E::Wire>,
        for<'d> A: app::Borrowed<'d, ID, Wire = E::Wire>
            + app::Lifecycle<'d, 'd, ID>
            + app::Scheduling<'d, ID>,
        for<'p, 'source, 'd> Host<engine::Machine<'p, 'd, 'd, ID, A, queue::Control<'source, 'd, tcp::Tcp, ID>, E>>:
            executor::raw::App<'d>,
    {
        use tcp::Tcp;

        let Self {
            attempt: request,
            app,
            config,
            env: marker::PhantomData,
        } = self;
        use dope_core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let creation = executor::Executor::new(&mut create, config);
        let executor = match creation {
            Ok(executor) => Ok(executor),
            Err(ref error) => Err(error.kind()),
        }
        .expect("native creation");
        let complete = &mut Completion::new();
        let mut reports = Reports::new(config, [ID::ROUTE]);
        let (output, held) = executor
            .with_plan(Attempts::<ID>::new(capacity::Count::new(1)))
            .run(|executor| {
                let result = executor.try_create(
                    move |mut session| {
                        let source = session.storage();
                        let options = Tcp::options(request.stream);
                        let target = attempt::StreamTarget::new(request.address, options);
                        let slot = source.try_reserve().map_err(Failure::Attempt)?;
                        let attempt = slot.start(target);
                        let output = engine::Startup::new(
                            app,
                            source,
                            Capacity::fixed::<1>(),
                            egress::Config::default(),
                            binding,
                        )
                        .try_with::<E, _, _>(
                            &mut session,
                            session::Scope::driver,
                            |engine, session| {
                                session.with_quiesce(
                                    complete,
                                    Host::new(engine),
                                    Duration::from_secs(2),
                                    |mut app| body(&mut app),
                                )
                            },
                        )
                        .map_err(Failure::Engine)?;
                        let held = !source.has_room();
                        attempt.cancel();
                        Ok::<_, Failure>((output, held))
                    },
                    recovery::Policy::new(|event| reports.push(event), crate::common::recover),
                );
                match result {
                    Ok(output) => Ok(output),
                    Err(ref error) => Err(Kind::from(error)),
                }
                .expect("native lifecycle")
            })
            .expect("attempt admission");
        assert!(held, "terminal attempt lease: {output:?}");
        super::checked((output, reports));
    }
}

/// One driver-owned completion queue for attempt conformance tests.
pub struct Mailbox<O> {
    capacity: capacity::Count,
    outcome: marker::PhantomData<fn() -> O>,
}

impl<O> Mailbox<O> {
    pub const fn new(capacity: capacity::Count) -> Self {
        Self {
            capacity,
            outcome: marker::PhantomData,
        }
    }
}

impl<O> settings::Resource for Mailbox<O> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(claim::Claim::new(settings::Routes::singleton::<KeyTag<0>>()))
    }
}

impl<O> storage::Plan for Mailbox<O> {
    type Output<'p, 'd>
        = mailbox::Domain<'d, tcp::Tcp, u64, O>
    where
        Self: 'd,
        'd: 'p;
}

impl<O> storage::Create for Mailbox<O> {
    type Error = creation::Error;

    fn try_build<'p, 'd>(
        self,
        mut context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        mailbox::Domain::try_new(self.capacity, &mut context)
    }
}
