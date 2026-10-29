use dope_test::harness::{observe::VecCellExt as _, scenario::TIMEOUT};

pub mod server;
mod start;
mod until;
use std::{
    cell::Cell,
    iter,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsFd as _,
    pin,
    rc::Rc,
    time::Duration,
};

use dope::{
    core::{
        dope_sys::io::socket::record::Fault,
        driver::{
            route::KeyTag,
            schedule::{Progress, Turn, clock},
        },
        platform::{channel::Channel, poll, wake},
    },
    manifold::{
        app::{creation, dispatch},
        service::discover::{Action, PollAt as _},
    },
    runtime::{
        executor::{drive, session},
        topology::lending,
    },
};
use dope_dns::{
    Report, Target, TargetError, config,
    lookup::{Discovery, Metadata, create, error, refresh},
    storage,
    topology::Plan,
};
use o3::{alloc, base::cell::region};
use pin_project::pin_project;
use start::Start;
pub use start::Stop;
pub(crate) use until::Until;

use crate::common::{Owner, fixture};

pub const DATAGRAM_ROUTE: u8 = 0;
pub const STREAM_ROUTE: u8 = 1;
const CLOCK_ROUTE: u8 = 4;
pub const LANES: usize = 1;
pub const ADDRESSES: usize = 4;
pub const OUTBOUND: usize = 2;

#[derive(Clone)]
pub struct Run {
    pub resolved: Rc<Cell<Vec<Vec<SocketAddr>>>>,
    pub published: Rc<Cell<Vec<Metadata>>>,
    pub failures: Rc<Cell<u32>>,
    pub fault: Rc<Cell<Option<Advance>>>,
    pub changes: Rc<Cell<u32>>,
    pub allow_failures: bool,
    pub stop_after: usize,
}

impl Run {
    pub fn new(allow: bool, limit: usize) -> Self {
        Self {
            resolved: Rc::new(Cell::new(Vec::new())),
            published: Rc::new(Cell::new(Vec::new())),
            failures: Rc::new(Cell::new(0)),
            fault: Rc::new(Cell::new(None)),
            changes: Rc::new(Cell::new(0)),
            allow_failures: allow,
            stop_after: limit,
        }
    }

    pub fn try_tail<'report, 'a>(
        &self,
        finish: &'report mut session::Completion,
        signal: &'a Channel,
        delay: Option<Duration>,
    ) -> Result<Tail<'report, 'a>, alloc::Error> {
        Ok(Tail {
            finish,
            run: self.clone(),
            signal,
            events: poll::Set::try_new(iter::once(signal.as_fd()), iter::empty())?,
            delay,
        })
    }
}

pub fn try_configure(
    servers: config::Servers,
    max: u64,
    stale: u64,
    delay: Option<Duration>,
) -> Option<config::Config> {
    let mut config = fixture::configure(servers, 1);
    let timeout = match delay {
        Some(delay) => delay.checked_add(Duration::from_secs(1))?,
        None => Duration::from_millis(20),
    };
    config.query.attempt_timeout = timeout;
    config.query.total_timeout = timeout;
    config.refresh.minimum = Duration::from_millis(1);
    config.refresh.backoff.base = Duration::from_millis(1);
    config.refresh.backoff.max = Duration::from_millis(1);
    config.refresh.maximum_ttl = Duration::from_millis(max);
    config.refresh.before = Duration::from_millis(max);
    config.refresh.stale = Duration::from_millis(stale);
    Some(config)
}

#[pin_project]
pub(crate) struct Resolution<'storage, 's, 'd, B> {
    pub(crate) discovery: Discovery<'storage, 's, 'd, refresh::Transport, LANES, ADDRESSES, B>,
    pub(crate) run: Run,
    pub(crate) next: Rc<Cell<Option<clock::Deadline<'d>>>>,
}

impl<B> Resolution<'_, '_, '_, B> {
    fn poll<'d>(
        query: &mut Discovery<'_, '_, 'd, refresh::Transport, LANES, ADDRESSES, B>,
        run: &mut Run,
        now: clock::Moment<'d>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Option<clock::Deadline<'d>> {
        if run.resolved.len() == run.stop_after {
            return None;
        }
        let (value, poll_at) = match query.poll_at(token, now) {
            Action::Pending { poll_at } => return poll_at,
            Action::Stale { poll_at, .. } => {
                run.changes.set(run.changes.get() + 1);
                return poll_at;
            }
            Action::Published {
                snapshot,
                metadata,
                poll_at,
            } => {
                run.published.push(metadata);
                (
                    snapshot
                        .endpoints()
                        .iter()
                        .map(|endpoint| *endpoint.id())
                        .collect(),
                    poll_at,
                )
            }
            Action::Expired { poll_at, .. } => (Vec::new(), poll_at),
            Action::Failed { error, poll_at } => {
                if !run.allow_failures
                    || !matches!(
                        &error,
                        error::Error::Query {
                            kind: error::Kind::Timeout,
                            ..
                        }
                    )
                {
                    Advance::Query(error).record(&run.fault);
                }
                run.failures.set(run.failures.get() + 1);
                run.changes.set(run.changes.get() + 1);
                return poll_at;
            }
        };
        run.resolved.push(value);
        run.changes.set(run.changes.get() + 1);
        poll_at
    }
}

#[pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'app, 's, 'd, B> {
    #[pin]
    #[manifold]
    resolution: Resolution<'app, 's, 'd, B>,
}

#[derive(Debug)]
pub enum Advance {
    Drive(drive::Error),
    Deadline,
    Clock(clock::Range),
    Resume(Fault),
    Signal(server::Failure),
    Query(error::Error<ADDRESSES>),
    Readiness,
    Wait(wake::WaitError),
    Notify(wake::NotifyError),
}

impl Advance {
    pub(crate) fn record(self, fault: &Cell<Option<Self>>) {
        if let Some(first) = fault.replace(Some(self)) {
            fault.set(Some(first));
        }
    }
}

#[derive(Debug)]
pub enum Failure<'report> {
    Install(creation::Error),
    Target(TargetError),
    Lookup(create::Error),
    Advance(Advance),
    Session(session::Error<'report, Result<(), Advance>>),
}

impl<'report> From<lending::Error<'report, create::Error, Result<(), Advance>>>
    for Failure<'report>
{
    fn from(error: lending::Error<'report, create::Error, Result<(), Advance>>) -> Self {
        match error {
            lending::Error::Build(error) => Self::Lookup(error),
            lending::Error::Run(error) => Self::Session(error),
        }
    }
}

pub struct Tail<'report, 'a> {
    pub finish: &'report mut session::Completion,
    pub run: Run,
    pub signal: &'a Channel,
    pub events: poll::Set<'a>,
    pub delay: Option<Duration>,
}

const fn v4(a: u8, b: u8, c: u8, d: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), 443)
}

pub const FIRST: [SocketAddr; 1] = [v4(10, 1, 105, 164)];
pub const NEXT: [SocketAddr; 2] = [v4(10, 1, 84, 243), v4(10, 1, 108, 45)];
pub const BOTH: [SocketAddr; 2] = [
    v4(10, 1, 105, 164),
    SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 443),
];

unsafe impl<'app, 's: 'app, 'd: 'app, B> dispatch::raw::Manifold<'d>
    for Resolution<'app, 's, 'd, B>
{
    type Route = KeyTag<CLOCK_ROUTE>;
    type Dispatch = dispatch::raw::Omit;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Omit;
    type Activate = dispatch::raw::Omit;
    type Probe = Self;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let this = self.as_mut().project();
        let now = turn.clock().now();
        let token = driver.state_token();
        if !this.discovery.is_ready(token)
            && this.next.get().is_none_or(|deadline| !deadline.is_due(now))
        {
            return self;
        }
        let poll_at = Self::poll(this.discovery, this.run, now, token);
        if this.discovery.is_ready(token) {
            Advance::Readiness.record(&this.run.fault);
        }
        this.next.set(poll_at);
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        _: Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self.as_mut().project().next.set(None);
        self
    }
}

// SAFETY: unit selection binds every query to this exact pinned resolution.
unsafe impl<'app, 's: 'app, 'd: 'app, B> dispatch::raw::Probe<'d, Self>
    for Resolution<'app, 's, 'd, B>
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> Progress<'d> {
        let this = call.owner().project_ref();
        if this.discovery.is_ready(call.state()) {
            return Progress::Runnable;
        }
        match this.next.get() {
            Some(deadline) => Progress::until(deadline),
            None => Progress::Quiescent,
        }
    }
}

pub fn compose<'report, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, R: Report + 'd>(
    tail: Tail<'report, '_>,
    install: Plan<KeyTag<DATAGRAM_ROUTE>, KeyTag<STREAM_ROUTE>, OUTBOUND, LANES, ADDRESSES, R>,
    stage: &mut lending::Stage<'_, 'scope, 'life, 'e, 'd, storage::Plan<LANES, ADDRESSES>, ()>,
    outcome: &mut Option<Result<(), Failure<'report>>>,
) {
    let installed = lending::Installer::install(
        install,
        stage,
        |owner: Owner<'_, '_, '_, 'life, 'e, 'd, R>,
         stage: &mut lending::Stage<
            '_,
            'scope,
            'life,
            'e,
            'd,
            storage::Plan<LANES, ADDRESSES>,
            (),
        >| {
            let target = match Target::parse("service.example:443") {
                Ok(target) => target,
                Err(error) => {
                    *outcome = Some(Err(Failure::Target(error)));
                    return;
                }
            };
            let Tail {
                run,
                signal,
                events,
                delay,
                finish,
            } = tail;
            let now = stage.driver().driver_ref().clock().now();
            let build = Start {
                target,
                run,
                signal,
                events,
                delay,
                now,
            };
            *outcome = Some(
                match stage.lend_at(finish, owner, build, now.into(), TIMEOUT) {
                    Ok(output) => output.map_err(Failure::Advance),
                    Err(error) => Err(Failure::from(error)),
                },
            );
        },
    );
    if let Err(error) = installed {
        *outcome = Some(Err(Failure::Install(error)));
    }
}

pub fn stop<'scope, 'life: 'scope, 'e: 'life, 'd: 'e, R: Report + 'd>(
    build: Stop,
    finish: &mut session::Completion,
    install: Plan<KeyTag<DATAGRAM_ROUTE>, KeyTag<STREAM_ROUTE>, OUTBOUND, LANES, ADDRESSES, R>,
    stage: &mut lending::Stage<'_, 'scope, 'life, 'e, 'd, storage::Plan<LANES, ADDRESSES>, ()>,
) {
    lending::Installer::install(
        install,
        stage,
        |owner: Owner<'_, '_, '_, 'life, 'e, 'd, R>,
         stage: &mut lending::Stage<
            '_,
            'scope,
            'life,
            'e,
            'd,
            storage::Plan<LANES, ADDRESSES>,
            (),
        >| {
            stage
                .lend_for(finish, owner, build, TIMEOUT)
                .expect("pending DNS shutdown")
                .expect("DNS application");
        },
    )
    .expect("DNS install");
}
