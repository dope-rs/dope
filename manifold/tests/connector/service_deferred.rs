use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    net::{SocketAddr, TcpListener},
    rc::Rc,
    time::Duration,
};

use dope_core::{driver::settings, io::socket::option};
use dope_manifold::{
    Bundle,
    connector::{
        codec::{Codec, Parse},
        lifecycle::{CloseReason, Stateless},
        session::{Ctx, Retirement, Scheduling, Session, Target},
    },
    service::{self, Endpoint, Revision, Snapshot},
    timing::Throughput,
};
use dope_net::{Transport, tcp::Tcp};
use dope_runtime::executor::Executor;
use dope_test::{fibers::Gate, scenario::ManifoldHost};
use o3::buffer::storage::Shared;

use crate::open_failure::DeferredWire;

struct CountedAddress {
    socket: SocketAddr,
    resolutions: Rc<Cell<usize>>,
}

struct CountedTransport;

impl Transport for CountedTransport {
    type Addr = CountedAddress;
    type StreamConfig = ();

    fn to_sock_addr(addr: &Self::Addr) -> std::io::Result<dope_core::io::socket::Addr> {
        addr.resolutions.set(addr.resolutions.get() + 1);
        Tcp::to_sock_addr(&addr.socket)
    }

    fn stream_options(_: Self::StreamConfig) -> std::io::Result<option::StreamOptions> {
        Ok(option::StreamOptions::default())
    }
}

struct NeedMore;

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

struct TestSession {
    codec: NeedMore,
    connected: Gate,
    peers: Rc<RefCell<Vec<SocketAddr>>>,
}

impl<'d> Session<'d> for TestSession {
    type Codec = NeedMore;
    type ConnState = Stateless;
    type Send = Shared;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, peer: dope_core::io::socket::Addr, _context: &mut Ctx<'_, 'd, Self>) {
        self.peers
            .borrow_mut()
            .push(peer.into_std().expect("TCP peer address"));
        self.connected.hit();
    }

    fn response<'input>(&mut self, _head: (), _context: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
    }
}

impl<'d> Retirement<'d> for TestSession {
    fn disconnect(&mut self, _context: &mut Ctx<'_, 'd, Self>, _reason: CloseReason) {}
}

impl<'d> Scheduling<'d> for TestSession {}

impl<'d> Target<'d, 0, 1> for TestSession {}

type TestSnapshot = Snapshot<Endpoint<SocketAddr, CountedAddress>>;

struct Switch {
    next: Rc<RefCell<Option<TestSnapshot>>>,
}

#[derive(Clone)]
struct SwitchHandle(Rc<RefCell<Option<TestSnapshot>>>);

impl Switch {
    fn new(snapshot: TestSnapshot) -> (Self, SwitchHandle) {
        let next = Rc::new(RefCell::new(Some(snapshot)));
        (Self { next: next.clone() }, SwitchHandle(next))
    }
}

impl SwitchHandle {
    fn publish(&self, snapshot: TestSnapshot) {
        assert!(
            self.0.borrow_mut().replace(snapshot).is_none(),
            "discovery publication was not consumed"
        );
    }
}

impl service::discover::Discover<SocketAddr, CountedAddress, 16> for Switch {
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
    ) -> service::discover::Action<SocketAddr, CountedAddress, (), Infallible, 16> {
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

type Env = Bundle<CountedTransport, DeferredWire, Throughput>;
type BorrowedFixedConnector<'d, 'i> = service::connector::Connector<
    'd,
    0,
    1,
    TestSession,
    service::Fixed<&'i str, CountedAddress>,
    service::reconcile::Preserve,
    &'i str,
    service::observe::Ignore,
    Env,
>;
type SwitchingConnector<'d> = service::connector::Connector<
    'd,
    0,
    1,
    TestSession,
    Switch,
    service::reconcile::Replace,
    SocketAddr,
    service::observe::Ignore,
    Env,
>;

fn snapshot(revision: u64, socket: SocketAddr, resolutions: Rc<Cell<usize>>) -> TestSnapshot {
    Snapshot::try_new(
        Revision::new(revision),
        [Endpoint::new(
            socket,
            CountedAddress {
                socket,
                resolutions,
            },
        )],
    )
    .expect("single endpoint snapshot")
}

fn borrowed_snapshot(
    revision: u64,
    identity: &str,
    socket: SocketAddr,
    resolutions: Rc<Cell<usize>>,
) -> Snapshot<Endpoint<&str, CountedAddress>> {
    Snapshot::try_new(
        Revision::new(revision),
        [Endpoint::new(
            identity,
            CountedAddress {
                socket,
                resolutions,
            },
        )],
    )
    .expect("single borrowed endpoint snapshot")
}

#[test]
fn service_deferred_head_reuses_the_owned_plan() {
    let (address, server) = dope_test::peer::Peer::hold(1);
    let identity = String::from("borrowed-service");
    let resolutions = Rc::new(Cell::new(0));
    let connected = Gate::new();
    let peers = Rc::new(RefCell::new(Vec::new()));
    let wire = DeferredWire::default();

    Executor::new(settings::Config::for_tcp_profile::<Throughput>(1).expect("driver config"))
        .expect("executor")
        .with_storage(())
        .enter(|mut runtime| {
            let backoff = runtime.hash_state(service::health::Domain::DEFAULT);
            let connector = BorrowedFixedConnector::new(
                TestSession {
                    codec: NeedMore,
                    connected: connected.clone(),
                    peers: peers.clone(),
                },
                service::Fixed::new(borrowed_snapshot(
                    1,
                    identity.as_str(),
                    address,
                    resolutions.clone(),
                )),
                service::connector::Config::new(
                    1,
                    service::health::Backoff::new(Duration::from_millis(10), backoff)
                        .expect("valid backoff"),
                    service::observe::Ignore,
                    wire.clone(),
                ),
                &mut runtime.driver_access(),
            )
            .expect("service connector");
            runtime
                .with_app(ManifoldHost::new(connector), |mut app| {
                    dope_test::fibers::TEST.run_until(&mut app, &connected, 1);
                })
                .expect("application teardown");
        });

    assert_eq!(wire.opens(), 2);
    assert_eq!(resolutions.get(), 1);
    assert_eq!(peers.borrow().as_slice(), &[address]);
    server.join().expect("server join");
}

#[test]
fn reconcile_rebinds_a_deferred_head_before_retry() {
    let old_listener = TcpListener::bind("127.0.0.1:0").expect("bind old endpoint");
    let old_address = old_listener.local_addr().expect("old endpoint address");
    let (new_address, new_server) = dope_test::peer::Peer::hold(1);
    let old_resolutions = Rc::new(Cell::new(0));
    let new_resolutions = Rc::new(Cell::new(0));
    let connected = Gate::new();
    let peers = Rc::new(RefCell::new(Vec::new()));
    let wire = DeferredWire::default();
    let deferred = wire.deferred().clone();
    let (discovery, discovery_handle) =
        Switch::new(snapshot(1, old_address, old_resolutions.clone()));
    let replacement = snapshot(2, new_address, new_resolutions.clone());

    Executor::new(settings::Config::for_tcp_profile::<Throughput>(1).expect("driver config"))
        .expect("executor")
        .with_storage(())
        .enter(|mut runtime| {
            let backoff = runtime.hash_state(service::health::Domain::DEFAULT);
            let connector = SwitchingConnector::new(
                TestSession {
                    codec: NeedMore,
                    connected: connected.clone(),
                    peers: peers.clone(),
                },
                discovery,
                service::connector::Config::new(
                    1,
                    service::health::Backoff::new(Duration::from_millis(10), backoff)
                        .expect("valid backoff"),
                    service::observe::Ignore,
                    wire.clone(),
                ),
                &mut runtime.driver_access(),
            )
            .expect("service connector");
            runtime
                .with_app(ManifoldHost::new(connector), |mut app| {
                    dope_test::fibers::TEST.run_until(&mut app, &deferred, 1);
                    discovery_handle.publish(replacement);
                    dope_test::fibers::TEST.run_until(&mut app, &connected, 1);
                })
                .expect("application teardown");
        });

    assert_eq!(wire.opens(), 2);
    assert_eq!(old_resolutions.get(), 1);
    assert_eq!(new_resolutions.get(), 1);
    assert_eq!(peers.borrow().as_slice(), &[new_address]);
    drop(old_listener);
    new_server.join().expect("new server join");
}
