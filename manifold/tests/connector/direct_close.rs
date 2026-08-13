use std::{
    cell::RefCell,
    convert::Infallible,
    io::Write,
    net::{SocketAddr, TcpListener},
    num::NonZeroUsize,
    rc::Rc,
    thread::{self, JoinHandle},
    time::Duration,
};

use dope_core::driver::{schedule, settings};
use dope_manifold::{
    connector::{
        codec::{Codec, Parse},
        connection,
        lifecycle::{CloseReason, Stateless},
        session::{Ctx, Retirement, Scheduling, Session, Target},
    },
    service::{self, Change, Endpoint, ReconcileError, Revision, Snapshot},
    timing::Balanced,
};
use dope_runtime::executor::Executor;
use o3::buffer::storage::Shared;

struct RejectBytes;

struct NeedMore;

struct Overconsume;

struct CapacityExhausted;

impl Codec for CapacityExhausted {
    type Head<'input, 'd> = ();
    type ParseState = ();
    type Error = Infallible;

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: dope_net::wire::Cursor<'d>>(
        &self,
        _state: &mut Self::ParseState,
        _buf: dope_manifold::connector::codec::Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        Ok(Parse::CapacityExhausted)
    }

    fn finish<'d>(
        &self,
        _state: &mut Self::ParseState,
        _remaining: dope_net::wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        Ok(None)
    }
}

impl Codec for NeedMore {
    type Head<'input, 'd> = ();
    type ParseState = ();
    type Error = Infallible;

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: dope_net::wire::Cursor<'d>>(
        &self,
        _state: &mut Self::ParseState,
        _buf: dope_manifold::connector::codec::Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        Ok(Parse::NeedMore)
    }

    fn finish<'d>(
        &self,
        _state: &mut Self::ParseState,
        _remaining: dope_net::wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        Ok(None)
    }
}

impl Codec for RejectBytes {
    type Head<'input, 'd> = ();
    type ParseState = ();
    type Error = ();

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: dope_net::wire::Cursor<'d>>(
        &self,
        _state: &mut Self::ParseState,
        buf: dope_manifold::connector::codec::Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        if buf.is_empty() {
            Ok(Parse::NeedMore)
        } else {
            Err(())
        }
    }

    fn finish<'d>(
        &self,
        _state: &mut Self::ParseState,
        remaining: dope_net::wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        if remaining.is_empty() {
            Ok(None)
        } else {
            Err(())
        }
    }
}

impl Codec for Overconsume {
    type Head<'input, 'd> = ();
    type ParseState = ();
    type Error = Infallible;

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: dope_net::wire::Cursor<'d>>(
        &self,
        _state: &mut Self::ParseState,
        _buf: dope_manifold::connector::codec::Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        Ok(Parse::Item {
            head: (),
            consumed: NonZeroUsize::MAX,
        })
    }

    fn finish<'d>(
        &self,
        _state: &mut Self::ParseState,
        _remaining: dope_net::wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        Ok(None)
    }
}

#[derive(Default)]
struct Events<'d> {
    connected: Vec<connection::Id<'d, 0>>,
    peers: Vec<SocketAddr>,
    closed: Vec<(connection::Id<'d, 0>, CloseReason)>,
    responses: usize,
}

struct ReconcileSession<'d> {
    codec: NeedMore,
    events: Rc<RefCell<Events<'d>>>,
    connected: dope_test::fibers::Gate,
}

struct FaultSession<'d, C> {
    codec: C,
    events: Rc<RefCell<Events<'d>>>,
    connected: dope_test::fibers::Gate,
}

impl<'d, C: for<'input, 'driver> Codec<Head<'input, 'driver> = ()>> Session<'d>
    for FaultSession<'d, C>
{
    type Codec = C;
    type ConnState = Stateless;
    type Send = Shared;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, peer: dope_core::io::socket::Addr, context: &mut Ctx<'_, 'd, Self>) {
        let mut events = self.events.borrow_mut();
        events.connected.push(context.conn_id);
        events
            .peers
            .push(peer.into_std().expect("TCP peer address"));
        self.connected.hit();
    }

    fn response<'input>(&mut self, _head: (), _context: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
        self.events.borrow_mut().responses += 1;
    }
}

impl<'d, C: for<'input, 'driver> Codec<Head<'input, 'driver> = ()>> Retirement<'d>
    for FaultSession<'d, C>
{
    fn disconnect(&mut self, context: &mut Ctx<'_, 'd, Self>, reason: CloseReason) {
        self.events
            .borrow_mut()
            .closed
            .push((context.conn_id, reason));
    }
}

impl<'d, C: for<'input, 'driver> Codec<Head<'input, 'driver> = ()>> Scheduling<'d>
    for FaultSession<'d, C>
{
}

impl<'d, C: for<'input, 'driver> Codec<Head<'input, 'driver> = ()>> Target<'d, 0, 1>
    for FaultSession<'d, C>
{
}

const RECONCILE_CONNECTIONS: usize = schedule::MAX_TURN_WORK_BUDGET + 1;

impl<'d> Session<'d> for ReconcileSession<'d> {
    type Codec = NeedMore;
    type ConnState = Stateless;
    type Send = Shared;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, peer: dope_core::io::socket::Addr, context: &mut Ctx<'_, 'd, Self>) {
        let mut events = self.events.borrow_mut();
        events.connected.push(context.conn_id);
        events
            .peers
            .push(peer.into_std().expect("TCP peer address"));
        self.connected.hit();
    }

    fn response<'input>(&mut self, _head: (), _context: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
    }
}

impl<'d> Retirement<'d> for ReconcileSession<'d> {
    fn disconnect(&mut self, context: &mut Ctx<'_, 'd, Self>, reason: CloseReason) {
        self.events
            .borrow_mut()
            .closed
            .push((context.conn_id, reason));
    }
}

impl<'d> Scheduling<'d> for ReconcileSession<'d> {}

impl<'d> Target<'d, 0, RECONCILE_CONNECTIONS> for ReconcileSession<'d> {}
impl<'d> Target<'d, 0, 1> for ReconcileSession<'d> {}

type FaultConnector<'d, C> = service::connector::Connector<
    'd,
    0,
    1,
    FaultSession<'d, C>,
    service::Fixed<SocketAddr, SocketAddr>,
    service::reconcile::Preserve,
>;

fn fixed_snapshot(addr: SocketAddr) -> Snapshot<Endpoint<SocketAddr, SocketAddr>> {
    Snapshot::try_new(Revision::new(1), [Endpoint::new(addr, addr)])
        .expect("valid service snapshot")
}

type ServiceSnapshot = Snapshot<Endpoint<&'static str, SocketAddr>>;

struct Switch {
    next: Rc<RefCell<Option<ServiceSnapshot>>>,
}

#[derive(Clone)]
struct SwitchHandle(Rc<RefCell<Option<ServiceSnapshot>>>);

impl Switch {
    fn new(snapshot: ServiceSnapshot) -> (Self, SwitchHandle) {
        let next = Rc::new(RefCell::new(Some(snapshot)));
        (Self { next: next.clone() }, SwitchHandle(next))
    }
}

impl SwitchHandle {
    fn publish(&self, snapshot: ServiceSnapshot) {
        assert!(
            self.0.borrow_mut().replace(snapshot).is_none(),
            "discovery publication was not consumed"
        );
    }
}

impl service::discover::Discover<&'static str, SocketAddr, 16> for Switch {
    type Metadata = ();
    type Error = Infallible;

    const REACTIVE: bool = true;

    fn changed(&self) -> bool {
        self.next.borrow().is_some()
    }

    fn refresh(&mut self, _now: std::time::Instant, _reason: service::discover::Refresh) {}

    fn poll(
        &mut self,
        _now: std::time::Instant,
    ) -> service::discover::Action<&'static str, SocketAddr, (), Infallible, 16> {
        match self.next.borrow_mut().take() {
            Some(snapshot) => service::discover::Action::Published {
                snapshot,
                metadata: (),
                poll_at: None,
            },
            None => service::discover::Action::Pending { poll_at: None },
        }
    }
}

#[derive(Default)]
struct ReconcileEvents {
    outcomes: Vec<Result<Change, ReconcileError>>,
}

struct Recorder {
    events: Rc<RefCell<ReconcileEvents>>,
    reconciled: dope_test::fibers::Gate,
    rejected: dope_test::fibers::Gate,
}

impl service::observe::Observe<&'static str, SocketAddr, (), Infallible, 16> for Recorder {
    fn resolved(
        &mut self,
        _snapshot: &Snapshot<Endpoint<&'static str, SocketAddr>>,
        _metadata: &(),
    ) {
    }

    fn expired(&mut self, _revision: Revision) {}

    fn reconciled(&mut self, _revision: Revision, change: Change) {
        self.events.borrow_mut().outcomes.push(Ok(change));
        self.reconciled.hit();
    }

    fn failed(&mut self, _error: &Infallible) {}

    fn rejected(&mut self, error: ReconcileError) {
        self.events.borrow_mut().outcomes.push(Err(error));
        self.rejected.hit();
    }
}

type ReconcileConnector<'d> = service::connector::Connector<
    'd,
    0,
    RECONCILE_CONNECTIONS,
    ReconcileSession<'d>,
    Switch,
    service::reconcile::Replace,
    &'static str,
    Recorder,
>;

type PreserveConnector<'d> = service::connector::Connector<
    'd,
    0,
    1,
    ReconcileSession<'d>,
    Switch,
    service::reconcile::Preserve,
    &'static str,
    Recorder,
>;

fn service_snapshot(
    revision: u64,
    endpoints: impl IntoIterator<Item = (&'static str, SocketAddr)>,
) -> ServiceSnapshot {
    Snapshot::try_new(
        Revision::new(revision),
        endpoints
            .into_iter()
            .map(|(id, addr)| Endpoint::new(id, addr)),
    )
    .expect("valid service snapshot")
}

fn invalid_byte_then_hold() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("listener address");
    let peer = thread::spawn(move || {
        let (mut first, _) = listener.accept().expect("first connection");
        first.write_all(&[0xff]).expect("invalid protocol byte");
        let _second = listener.accept().expect("protocol-error reconnect");
        let _release = listener.accept().expect("test release connection");
    });
    (addr, peer)
}

fn assert_reconnect<C: for<'input, 'driver> Codec<Head<'input, 'driver> = ()>>(
    codec: C,
    expected: CloseReason,
) {
    let (addr, peer) = invalid_byte_then_hold();
    let connected = dope_test::fibers::Gate::new();

    Executor::new(settings::Config::for_tcp_profile::<Balanced>(1).expect("driver config"))
        .expect("executor")
        .with_storage(())
        .enter(|mut session| {
            let events = Rc::new(RefCell::new(Events::default()));
            let backoff = session.hash_state(service::health::Domain::DEFAULT);
            let connector = FaultConnector::new(
                FaultSession {
                    codec,
                    events: events.clone(),
                    connected: connected.clone(),
                },
                service::Fixed::new(fixed_snapshot(addr)),
                service::connector::Config::new(
                    1,
                    service::health::Backoff::new(Duration::from_millis(10), backoff)
                        .expect("valid backoff"),
                    service::observe::Ignore,
                    (),
                ),
                &mut session.driver_access(),
            )
            .expect("connector");
            session
                .with_app(
                    dope_test::scenario::ManifoldHost::new(connector),
                    |mut app| {
                        dope_test::fibers::TEST.run_until(&mut app, &connected, 2);
                        let events = events.borrow();
                        assert_eq!(events.closed.len(), 1);
                        assert_eq!(events.closed[0].1, expected);
                        let old = events.connected[0];
                        let new = events.connected[1];
                        assert_eq!(old.index(), new.index());
                        assert_ne!(old, new);
                        assert_eq!(events.responses, 0);
                    },
                )
                .expect("application teardown");
        });

    drop(dope_test::peer::Peer::at(addr).connect());
    peer.join().expect("peer join");
}

fn assert_protocol_reconnect<C: for<'input, 'driver> Codec<Head<'input, 'driver> = ()>>(codec: C) {
    assert_reconnect(codec, CloseReason::Protocol);
}

#[test]
fn parser_error_immediately_reconnects_with_protocol_reason() {
    assert_protocol_reconnect(RejectBytes);
}

#[test]
fn parser_rejects_a_codec_that_consumes_beyond_ingress() {
    assert_protocol_reconnect(Overconsume);
}

#[test]
fn parser_capacity_is_not_reported_as_a_peer_protocol_failure() {
    assert_reconnect(CapacityExhausted, CloseReason::Capacity);
}

#[test]
fn failed_endpoint_falls_through_to_the_next_current_endpoint() {
    let refused = TcpListener::bind("127.0.0.1:0").expect("reserve refused address");
    let refused_addr = refused.local_addr().expect("refused address");
    drop(refused);
    let (good_addr, server) = dope_test::peer::Peer::hold(1);
    let connected = dope_test::fibers::Gate::new();

    Executor::new(settings::Config::for_tcp_profile::<Balanced>(1).expect("driver config"))
        .expect("executor")
        .with_storage(())
        .enter(|mut session| {
            let events = Rc::new(RefCell::new(Events::default()));
            let backoff = session.hash_state(service::health::Domain::DEFAULT);
            let snapshot = Snapshot::try_new(
                Revision::new(1),
                [
                    Endpoint::new(refused_addr, refused_addr),
                    Endpoint::new(good_addr, good_addr),
                ],
            )
            .expect("two endpoint snapshot");
            let connector = FaultConnector::new(
                FaultSession {
                    codec: RejectBytes,
                    events: events.clone(),
                    connected: connected.clone(),
                },
                service::Fixed::new(snapshot),
                service::connector::Config::new(
                    1,
                    service::health::Backoff::new(Duration::from_millis(10), backoff)
                        .expect("valid backoff"),
                    service::observe::Ignore,
                    (),
                ),
                &mut session.driver_access(),
            )
            .expect("connector");
            session
                .with_app(
                    dope_test::scenario::ManifoldHost::new(connector),
                    |mut app| {
                        dope_test::fibers::TEST.run_until(&mut app, &connected, 1);
                    },
                )
                .expect("application teardown");
            assert_eq!(events.borrow().peers, [good_addr]);
        });

    server.join().expect("server join");
}

#[test]
fn discovery_rejects_atomically_then_replaces_every_retired_epoch() {
    let (old_addr, old_server) = dope_test::peer::Peer::hold(RECONCILE_CONNECTIONS + 1);
    let (new_addr, new_server) = dope_test::peer::Peer::hold(RECONCILE_CONNECTIONS + 1);
    let connected = dope_test::fibers::Gate::new();
    let reconciliation = Rc::new(RefCell::new(ReconcileEvents::default()));
    let reconciled = dope_test::fibers::Gate::new();
    let rejected = dope_test::fibers::Gate::new();

    Executor::new(
        settings::Config::for_tcp_profile::<Balanced>(RECONCILE_CONNECTIONS)
            .expect("driver config"),
    )
    .expect("executor")
    .with_storage(())
    .enter(|mut session| {
        let connections = Rc::new(RefCell::new(Events::default()));
        let backoff = session.hash_state(service::health::Domain::DEFAULT);
        let (discovery, discovery_handle) = Switch::new(service_snapshot(1, [("old", old_addr)]));
        let connector = ReconcileConnector::new(
            ReconcileSession {
                codec: NeedMore,
                events: connections.clone(),
                connected: connected.clone(),
            },
            discovery,
            service::connector::Config::new(
                RECONCILE_CONNECTIONS,
                service::health::Backoff::new(Duration::from_millis(10), backoff)
                    .expect("valid backoff"),
                Recorder {
                    events: reconciliation.clone(),
                    reconciled: reconciled.clone(),
                    rejected: rejected.clone(),
                },
                (),
            ),
            &mut session.driver_access(),
        )
        .expect("connector");

        session
            .with_app(
                dope_test::scenario::ManifoldHost::new(connector),
                |mut app| {
                    dope_test::fibers::TEST.run_until(
                        &mut app,
                        &connected,
                        RECONCILE_CONNECTIONS as u32,
                    );
                    let original = connections.borrow().connected.clone();
                    assert_eq!(
                        connections.borrow().peers,
                        vec![old_addr; RECONCILE_CONNECTIONS]
                    );

                    discovery_handle.publish(service_snapshot(1, [("old", new_addr)]));
                    dope_test::fibers::TEST.run_until(&mut app, &rejected, 1);

                    assert_eq!(connections.borrow().connected, original);
                    assert!(connections.borrow().closed.is_empty());

                    discovery_handle.publish(service_snapshot(
                        2,
                        [("duplicate", old_addr), ("duplicate", new_addr)],
                    ));
                    dope_test::fibers::TEST.run_until(&mut app, &rejected, 2);

                    assert_eq!(connections.borrow().connected, original);
                    assert!(connections.borrow().closed.is_empty());
                    assert_eq!(
                        reconciliation.borrow().outcomes.as_slice(),
                        &[
                            Ok(Change {
                                added: 1,
                                retained: 0,
                                retired: 0,
                            }),
                            Err(ReconcileError::Stale {
                                current: Revision::new(1),
                                incoming: Revision::new(1),
                            }),
                            Err(ReconcileError::Duplicate),
                        ]
                    );

                    discovery_handle.publish(service_snapshot(3, [("new", new_addr)]));
                    dope_test::fibers::TEST.run_until(&mut app, &reconciled, 2);
                    assert!(
                        connections.borrow().closed.len()
                            <= schedule::MAX_TURN_WORK_BUDGET.div_ceil(2),
                        "one turn exceeded the retirement maintenance share"
                    );
                    dope_test::fibers::TEST.run_until(
                        &mut app,
                        &connected,
                        (RECONCILE_CONNECTIONS * 2) as u32,
                    );

                    let events = connections.borrow();
                    assert_eq!(
                        &events.peers[RECONCILE_CONNECTIONS..],
                        vec![new_addr; RECONCILE_CONNECTIONS]
                    );
                    for replacement in &events.connected[RECONCILE_CONNECTIONS..] {
                        let retired = original
                            .iter()
                            .find(|retired| retired.index() == replacement.index())
                            .expect("replacement must reuse a retired connection index");
                        assert_ne!(
                            retired, replacement,
                            "replacement must use a fresh generation"
                        );
                    }
                    assert_eq!(events.closed.len(), RECONCILE_CONNECTIONS);
                    assert!(events.closed.iter().all(|(token, reason)| {
                        original.contains(token) && *reason == CloseReason::EndpointRetired
                    }));
                    assert_eq!(
                        reconciliation.borrow().outcomes.last(),
                        Some(&Ok(Change {
                            added: 1,
                            retained: 0,
                            retired: 1,
                        }))
                    );
                },
            )
            .expect("application teardown");
    });

    drop(dope_test::peer::Peer::at(old_addr).connect());
    drop(dope_test::peer::Peer::at(new_addr).connect());
    old_server.join().expect("old server join");
    new_server.join().expect("new server join");
}

#[test]
fn preserve_freezes_the_live_epoch_and_updates_the_next_dial() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind old peer");
    let old_addr = listener.local_addr().expect("old peer address");
    let (release_old, released) = std::sync::mpsc::channel();
    let old_server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("old connection");
        released.recv().expect("release old connection");
        drop(stream);
    });
    let (new_addr, new_server) = dope_test::peer::Peer::hold(2);
    let connected = dope_test::fibers::Gate::new();
    let reconciliation = Rc::new(RefCell::new(ReconcileEvents::default()));
    let reconciled = dope_test::fibers::Gate::new();
    let rejected = dope_test::fibers::Gate::new();

    Executor::new(settings::Config::for_tcp_profile::<Balanced>(1).expect("driver config"))
        .expect("executor")
        .with_storage(())
        .enter(|mut session| {
            let connections = Rc::new(RefCell::new(Events::default()));
            let backoff = session.hash_state(service::health::Domain::DEFAULT);
            let (discovery, discovery_handle) =
                Switch::new(service_snapshot(1, [("node", old_addr)]));
            let connector = PreserveConnector::new(
                ReconcileSession {
                    codec: NeedMore,
                    events: connections.clone(),
                    connected: connected.clone(),
                },
                discovery,
                service::connector::Config::new(
                    1,
                    service::health::Backoff::new(Duration::from_millis(10), backoff)
                        .expect("valid backoff"),
                    Recorder {
                        events: reconciliation.clone(),
                        reconciled: reconciled.clone(),
                        rejected,
                    },
                    (),
                ),
                &mut session.driver_access(),
            )
            .expect("connector");

            session
                .with_app(
                    dope_test::scenario::ManifoldHost::new(connector),
                    |mut app| {
                        dope_test::fibers::TEST.run_until(&mut app, &connected, 1);
                        let original = connections.borrow().connected[0];
                        assert_eq!(connections.borrow().peers, [old_addr]);

                        discovery_handle.publish(service_snapshot(2, [("node", new_addr)]));
                        dope_test::fibers::TEST.run_until(&mut app, &reconciled, 2);

                        assert_eq!(connections.borrow().connected, [original]);
                        assert!(connections.borrow().closed.is_empty());
                        assert_eq!(
                            reconciliation.borrow().outcomes,
                            [
                                Ok(Change {
                                    added: 1,
                                    retained: 0,
                                    retired: 0,
                                }),
                                Ok(Change {
                                    added: 0,
                                    retained: 1,
                                    retired: 0,
                                }),
                            ]
                        );

                        release_old.send(()).expect("release old connection");
                        dope_test::fibers::TEST.run_until(&mut app, &connected, 2);

                        let events = connections.borrow();
                        assert_eq!(events.peers, [old_addr, new_addr]);
                        assert_eq!(events.connected[1].index(), original.index());
                        assert_ne!(events.connected[1], original);
                        assert_eq!(
                            events.closed,
                            [(original, CloseReason::Remote)],
                            "the preserved epoch must observe the peer's orderly EOF"
                        );
                    },
                )
                .expect("application teardown");
        });

    drop(dope_test::peer::Peer::at(new_addr).connect());
    old_server.join().expect("old server join");
    new_server.join().expect("new server join");
}
