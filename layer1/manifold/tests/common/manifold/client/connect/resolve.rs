use std::{cell::Cell, convert::Infallible, net::SocketAddr, rc::Rc, time::Duration};

use client::topology::{factory::disabled, plan};
use dope_core::{
    driver::{
        self,
        route::KeyTag,
        settings,
        settings::{claim, errors::Claim},
    },
    io::socket::{self, option},
};
use dope_manifold::{
    app::env::Bundle,
    client::{
        app::{admit::Automatic, connect::Full, failure::Observed, request},
        attempt,
        close::Reason,
        conn, session,
        session::{
            Output, Parser, Plain, Retirement, Scheduling, Session, Target, app::drain::Ready,
            codec::mode, policy, policy::output, response, turns,
        },
    },
    network::client,
    timing::Throughput,
};
use dope_net::{
    Transport,
    link::{egress, event, pool::change::opening},
    wire::domain,
};
use dope_runtime::{
    executor,
    executor::session::{Completion, Error},
    topology::lending,
};
use dope_test::{
    fibers,
    harness::{
        observe::Trace,
        signals::{LocalGateRef, Progress as _},
    },
};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};
use session::ingress;

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{
        connect::{self, open::DeferredWire},
        error, flush,
    },
};

pub struct Addr {
    pub(crate) socket: SocketAddr,
    pub(crate) resolves: Rc<Cell<usize>>,
}

pub struct Net;

impl From<&Addr> for socket::Inet {
    fn from(addr: &Addr) -> Self {
        addr.resolves.set(addr.resolves.get() + 1);
        Self::from(&addr.socket)
    }
}

impl Transport for Net {
    type Addr = Addr;
    type Prepared = socket::Inet;
    type StreamConfig = ();

    fn options(_: Self::StreamConfig) -> option::Set {
        option::Set::default()
    }
}

pub struct State<'test> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) codec: codec::Pending,
    pub(crate) ready: LocalGateRef<'test>,
    pub(crate) peers: &'test Trace<socket::Addr, 4>,
}

impl<'d> Session<'d> for State<'_> {
    type Codec = codec::Pending;
    type Connect = Full;
    type Requests = request::Disabled;
    type ConnState = ();
    type Failure = Observed;
    type Send = Shared;
    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<'test, 'd> policy::Compatible<'d, 'd, DeferredWire, KeyTag<0>> for State<'test> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for State<'_> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for State<'_> {
    type Delivery = response::Direct;

    fn reject(
        &mut self,
        _: errors::Truncated,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct {
        use dope_manifold::client::close;

        ctx.request(close::Directive::reconnect(close::Reason::Protocol));
        response::Direct::Committed
    }

    fn deliver<'input, R: buffer::Cursor>(
        &mut self,
        head: Infallible,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        match head {}
    }
}

impl<'d> Output<'d> for State<'_> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let ready = owner.ready.hits();
            let empty = context.sink.is_empty();
            if ready != 1 || !empty {
                owner
                    .audit
                    .faults
                    .record(connect::Error::Flush { ready, empty }.into());
            }
        })
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<_>::Ready(|owner: &mut Self| {
            owner.audit.record_sent();
        })
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        use Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d> session::Connected<'d> for State<'_> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        peer: socket::Addr,
        _: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.peers.record(peer);
        self.ready.hit();
    }
}

impl<'d> session::Failed<'d> for State<'_> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for State<'_> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for State<'_> {
    type Drain = Ready;

    fn io_fail(
        &mut self,
        error: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let ready = self.ready.hits();
        if ready != 1 {
            self.audit
                .faults
                .record(connect::Error::Quarantine(ready).into());
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        let ready = self.ready.hits();
        let peers = self.peers.values().map(|peers| peers.len());
        if ready != 1 || !matches!(peers, Ok(1)) {
            self.audit
                .faults
                .record(connect::Error::Disconnect { ready, peers }.into());
        }
    }
}

impl<'d> Scheduling<'d> for State<'_> {}

impl<'d> Target<'d, KeyTag<0>, 1> for State<'_> {}

pub struct App<'test> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) ready: LocalGateRef<'test>,
    pub(crate) peers: &'test Trace<socket::Addr, 4>,
    pub(crate) wire: DeferredWire,
}

impl settings::Resource for App<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, Claim> {
        Ok(needs)
    }
}

impl<'test> disabled::Application<(), KeyTag<0>, 1, Bundle<Net, DeferredWire, Throughput>>
    for App<'test>
{
    type Input = plan::Local;
    type Session<'life, 'p, 'd>
        = State<'test>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        (): 'd,
        DeferredWire: 'd;
    type Source<'life, 'p, 'd>
        = ingress::None
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        (): 'd,
        DeferredWire: 'd;
    type Wire<'life, 'p, 'd>
        = domain::Local<'d, DeferredWire, KeyTag<0>>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        (): 'd,
        DeferredWire: 'd;
    type Error = Infallible;

    fn build<'life, 'p, 'd>(
        self,
        _: &'life (),
    ) -> Result<
        (
            session::Setup<'d, Self::Session<'life, 'p, 'd>, Self::Source<'life, 'p, 'd>>,
            Self::Wire<'life, 'p, 'd>,
        ),
        Self::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        (): 'd,
        DeferredWire: 'd,
    {
        Ok((
            session::Setup::new(
                State {
                    audit: self.audit,
                    codec: codec::Pending,
                    ready: self.ready,
                    peers: self.peers,
                },
                ingress::Plan::none(),
            ),
            domain::Local::new(self.wire),
        ))
    }
}

pub struct Wait<'report, 'a>(
    pub(crate) LocalGateRef<'a>,
    pub(crate) &'report mut Completion,
);

impl<'report, 'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, A>
    lending::Next<lending::Stage<'stage, 'scope, 'life, 'p, 'd, (), ()>, A> for Wait<'report, '_>
where
    A: executor::raw::App<'d>,
{
    type Output = Result<
        Result<fibers::Wait, fibers::Run>,
        Error<'report, Result<fibers::Wait, fibers::Run>>,
    >;

    fn run(
        self,
        host: A,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, (), ()>,
    ) -> Self::Output {
        stage.with_quiesce(self.1, host, Duration::from_secs(2), |mut app| {
            fibers::TEST.try_wait(&mut app, &self.0, 1)
        })
    }
}

pub fn deferred_revision<R: dope_manifold::service::sync::Policy>(previous: Option<SocketAddr>) {
    use crate::common::manifold::client::{
        Configure as _,
        finish::close::members::{Notice, Source, Until, Watch},
    };
    use dope_core::{
        driver::{
            life::{creation::Attempt, recovery},
            route::Id,
        },
        platform::wake,
    };
    use dope_manifold::{
        app::Host,
        client::session::Setup,
        network::client::topology::plan::{Plan, Spec},
        service::{Endpoint, Revision, Snapshot, health::Domain},
        timing::Balanced,
    };
    use dope_net::{tcp::Tcp, wire::domain::Local};
    use dope_runtime::executor::{Executor, session::Scope};
    use dope_test::{
        alloc::Tracker,
        harness::{
            GUARD,
            peer::TcpScript,
            signals::{Gate, LocalGate},
        },
        reclaim::Reports,
    };

    let mut finish = Completion::new();
    let (wait, release) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    TcpScript::with_many::<1, _>(move |_| {
            wait.wait_timeout(GUARD).expect("release deferred peer");
        }, |server| {
        let pending = Rc::new(Cell::new(Some(Snapshot::from_one(
            Revision::new(1),
            Endpoint::new(10, previous.unwrap_or(server.addr())),
        ))));
        let changes = Rc::new(Trace::new());
        let ready = LocalGate::new();
        let peers = Trace::new();
        let wire = DeferredWire::default();
        let audit = Rc::new(Report::default());
        wire.state.first.set(false);
        wire.state.blocked.set(true);
        let plan = Plan::<KeyTag<0>, 1>::new(
            1,
            Spec::new(Duration::from_secs(2), Duration::from_secs(2)).expect("retry bounds"),
        )
        .expect("one connection");
        let config = plan.configured().expect("deferred connector resources");
        let mut create = Attempt::new();
        let result = Executor::new(&mut create, config);
        result.as_ref().map_err(|error| error.kind()).expect("executor construction");
        let executor = result.ok().expect("executor construction");
        let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
        executor.run(|executor| {
            let result = executor.enter_with(
                |mut runtime| {
                    let setup = Setup::new(
                        State {
                            audit: Rc::clone(&audit),
                            codec: codec::Pending,
                            ready: ready.handle(),
                            peers: &peers,
                        },
                        ingress::Plan::none(),
                    );
                    let hash = runtime.hash_state(Domain::DEFAULT);
                    let result = plan.bind(Local::new(wire.clone()), hash).prepare::<
                        _, _, R, _, Bundle<Tcp, DeferredWire, Balanced>, 1, _, _,
                    >(
                        setup,
                        &mut runtime,
                        Scope::driver,
                        |prepared, runtime| {
                            let now = runtime.driver().driver_ref().clock().now();
                            let source = Source { pending: pending.clone(), failures: Gate::new() };
                            let watch = Watch(changes.clone());
                            let (host, allocs) = Tracker::counts(|| prepared.discover(source, watch, now));
                            assert_eq!(allocs, [0, 0], "attach admitted discovery storage");
                            let result = runtime.with_at_for(
                                &mut finish,
                                Host::new(host),
                                now.into(),
                                Duration::from_secs(2),
                                |mut app| {
                                    app.drive_for(now.into(), Duration::from_secs(2), Until(|| wire.opens() != 0))
                                        .expect("selected epoch is deferred").into_output();
                                    let snapshot = match previous {
                                        Some(_) => Snapshot::from_one(
                                            Revision::new(2),
                                            Endpoint::new(10, server.addr()),
                                        ),
                                        None => Snapshot::empty(Revision::new(2)),
                                    };
                                    pending.set(Some(snapshot));
                                    let next = now.clamp_add(Duration::from_millis(1));
                                    app.drive_for(next, Duration::from_secs(2), Until(|| {
                                        wire.opens() >= 2 && changes.values().expect("discovery observations").iter().any(|notice| {
                                            matches!(notice, Notice::Reconciled(rev, _) if *rev == Revision::new(2))
                                        })
                                    }))
                                        .expect("apply revised discovery").into_output();
                                    assert_eq!(ready.handle().hits(), 0);
                                    wire.state.blocked.set(false);
                                    let retry = now.clamp_add(Duration::from_millis(3));
                                    app.drive_for(retry, Duration::from_secs(2), Until(|| ready.handle().hits() == 1))
                                        .expect("revised epoch connects").into_output();
                                },
                            );
                            assert!(result.is_ok(), "deferred application teardown");
                        },
                    );
                    assert!(result.is_ok(), "install deferred connector");
                },
                recovery::Policy::new(
                    |event| reports.push(event),
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            );
            assert!(result.is_ok(), "deferred executor teardown");
        });
        audit.checked((0,0));
        assert_eq!(ready.handle().hits(), 1);
        assert!(
            matches!(reports.events(), Ok(events) if events.is_empty()),
            "deferred attempt is finalized"
        );
        assert!(peers.values().expect("recorded observations")
            .iter()
            .copied()
            .map(|peer| peer.into_std().expect("TCP peer address"))
            .eq([server.addr()]));
        release.notify().expect("release peer");
        assert!(server.finish().is_ok());
    }).expect("deferred peer");
}
