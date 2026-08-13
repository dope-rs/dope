//! Anchor test for app-initiated close intent (`CloseKind`).
//!
//! The receive path can already ask for a reconnecting vs a permanent close
//! (`ChunkOutcome::CloseReconnect` / `ClosePermanent`). This pins the SYMMETRIC
//! guarantee for a close an app requests OFF the receive path (a supervisor /
//! timer, via `Application::drain_requests`): `CloseKind::Reconnect` must redial,
//! `CloseKind::Permanent` must NOT (else a terminal fault redials into the same
//! doomed connection forever). Real reactor + real loopback: no unit tests.
//!
//! Lives in `dope/tests` (the layer that owns the connector) so a refactor keeps
//! it green.
use std::{
    cell::Cell,
    convert::Infallible,
    net,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use dope_core::driver::{Context, schedule};
use dope_manifold::{
    Bundle,
    connector::{
        app::{self, Application, ChunkOutcome, CloseKind, Requests},
        attempt::Id,
        codec::{Codec, Parse},
        connection,
        lifecycle::{CloseReason, Stateless},
        session::{Ctx, Retirement, Scheduling, Session, Target},
    },
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_test::{
    fibers::Gate,
    peer::Peer,
    scenario::scenarios::{AttemptConnector, Connector},
};
use o3::buffer::{bytes::Retainable, storage::Shared};

const MAX: usize = 1;
const REDIAL_BACKOFF: Duration = Duration::from_millis(20);

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

/// On the first established connection, asks the connector to close it with a
/// configured `CloseKind` (via `drain_requests`, i.e. off the receive path).
struct CloseKindSession {
    codec: NeedMore,
    kind: CloseKind,
    gate: Gate,
    closed: Gate,
    pending: Cell<bool>,
}

impl<'d> Session<'d> for CloseKindSession {
    type Codec = NeedMore;
    type ConnState = Stateless;
    type Send = Shared;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, _peer: dope_core::io::socket::Addr, _context: &mut Ctx<'_, 'd, Self>) {
        self.gate.hit();
        if self.gate.hits() == 1 {
            self.pending.set(true);
        }
    }

    fn response<'input>(&mut self, _head: (), _context: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
    }

    fn drain_requests(
        &self,
        _connection: connection::Id<'d, 0>,
        _state: &mut <Self::Codec as Codec>::ParseState,
        _drain: &mut app::RequestDrain<'_, 'd, Self::Send>,
        _region: &mut o3::cell::region::Token<'d>,
    ) -> Requests {
        if self.pending.replace(false) {
            Requests {
                close: Some(self.kind),
            }
        } else {
            Requests::default()
        }
    }
}

impl<'d> Retirement<'d> for CloseKindSession {
    fn disconnect(&mut self, _context: &mut Ctx<'_, 'd, Self>, _reason: CloseReason) {
        self.closed.hit();
    }
}

impl<'d> Scheduling<'d> for CloseKindSession {}

impl<'d> Target<'d, 0, MAX> for CloseKindSession {}

struct Run {
    connected: Gate,
    closed: Gate,
    accepted: usize,
}

fn held_peer() -> (
    net::SocketAddr,
    Arc<AtomicUsize>,
    mpsc::Receiver<usize>,
    mpsc::Sender<()>,
    thread::JoinHandle<()>,
) {
    let listener = net::TcpListener::bind("127.0.0.1:0").expect("bind held peer listener");
    listener
        .set_nonblocking(true)
        .expect("make held peer nonblocking");
    let address = listener.local_addr().expect("held peer address");
    let accepted = Arc::new(AtomicUsize::new(0));
    let server_accepted = Arc::clone(&accepted);
    let (accepts, accepted_rx) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut held = Vec::new();
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    held.push(stream);
                    let count = server_accepted.fetch_add(1, Ordering::Release) + 1;
                    accepts.send(count).expect("accept observer remains alive");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("accept held connector: {error}"),
            }
            if released.try_recv().is_ok() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
    });
    (address, accepted, accepted_rx, release, server)
}

fn run(kind: CloseKind) -> Run {
    let (addr, accepted, accepts, release, server) = held_peer();
    let connected = Gate::new();
    let closed = Gate::new();

    Connector::<MAX>::new(addr, REDIAL_BACKOFF).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        CloseKindSession {
            codec: NeedMore,
            kind,
            gate: connected.clone(),
            closed: closed.clone(),
            pending: Cell::new(false),
        },
        |case| {
            case.until(&connected, 1);
            case.until(&closed, 1);
            if matches!(kind, CloseKind::Reconnect) {
                case.until(&connected, 2);
            } else {
                case.pause(REDIAL_BACKOFF.saturating_mul(4));
            }
        },
    );

    let expected = if matches!(kind, CloseKind::Reconnect) {
        2
    } else {
        1
    };
    while accepted.load(Ordering::Acquire) < expected {
        accepts
            .recv_timeout(dope_test::GUARD)
            .expect("peer accepts each connected socket");
    }
    release.send(()).expect("release held peer");
    server.join().expect("server join");
    Run {
        connected,
        closed,
        accepted: accepted.load(Ordering::Acquire),
    }
}

struct YieldingApp {
    connected: Gate,
    closed: Gate,
    pending: Cell<bool>,
    yielded: Cell<bool>,
    resumed: Rc<Cell<bool>>,
}

struct BoundedDrainApp {
    completed: Gate,
    remaining: Cell<usize>,
    calls: Rc<Cell<usize>>,
    first_remaining: Rc<Cell<Option<usize>>>,
}

struct BoundedFront<'a>(&'a Cell<usize>);

impl<'a> app::RequestFront for BoundedFront<'a> {
    type Item = (Shared, &'a Cell<usize>);

    fn take(self) -> Self::Item {
        self.0.set(self.0.get() - 1);
        (Shared::copy_from_slice(b"x"), self.0)
    }
}

impl<'d, const ID: u8> Application<'d, ID> for BoundedDrainApp {
    type Conn = ();
    type Wire = Identity;
    type Send = Shared;
    type Input = dope_manifold::receive::Borrowed;

    fn connection(&self) -> Self::Conn {}
}

impl<'d, const ID: u8> app::Receive<'d, ID> for BoundedDrainApp {
    type Continuation = app::continuation::Complete;
}

impl<'d, const ID: u8> app::BorrowedReceive<'d, ID> for BoundedDrainApp {
    fn chunk<O, R: Retainable>(
        &mut self,
        _connection: connection::Ctx<'_, 'd, ID, Identity, (), O>,
        _egress: dope_net::link::egress::Queue<'_, 'd, 32>,
        _chunk: R,
        _driver: &mut Context<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::Ok
    }
}

impl<'d, const ID: u8> app::Lifecycle<'d, ID> for BoundedDrainApp {
    fn connected<O>(
        &mut self,
        _key: Id<'d, ID>,
        _peer: dope_core::io::socket::Addr,
        _connection: connection::Ctx<'_, 'd, ID, Identity, (), O>,
        _egress: dope_net::link::egress::Queue<'_, 'd, 32>,
        _driver: &mut Context<'_, 'd>,
    ) {
    }

    fn sent(&mut self, _connection: connection::Id<'d, ID>, _has_pending_egress: bool) {}

    fn close<O>(
        &mut self,
        _connection: connection::Ctx<'_, 'd, ID, Identity, (), O>,
        _egress: dope_net::link::egress::Queue<'_, 'd, 32>,
        reason: CloseReason,
        _driver: &mut Context<'_, 'd>,
    ) -> app::CloseOutcome {
        app::CloseOutcome::Complete(reason)
    }
}

impl<'d, const ID: u8> app::RequestSource<'d, ID> for BoundedDrainApp {
    fn drain_requests(
        &self,
        _connection: connection::Id<'d, ID>,
        _state: &mut Self::Conn,
        drain: &mut app::RequestDrain<'_, 'd, Self::Send>,
        driver: &mut Context<'_, 'd>,
    ) -> Requests {
        let call = self.calls.get();
        self.calls.set(call + 1);
        while self.remaining.get() != 0 {
            let ((value, remaining), permit) =
                match drain.admit(Some(BoundedFront(&self.remaining))) {
                    app::RequestAdmission::Item(item, permit) => (item, permit),
                    app::RequestAdmission::Empty | app::RequestAdmission::Exhausted => break,
                };
            if let Err(value) = permit.try_push(driver.region_token(), value) {
                remaining.set(remaining.get() + 1);
                drop(value);
                break;
            }
        }
        if call == 0 {
            self.first_remaining.set(Some(self.remaining.get()));
        }
        if self.remaining.get() == 0 {
            self.completed.hit();
        }
        Requests::default()
    }
}

impl<'d, const ID: u8> app::Scheduling<'d, ID> for BoundedDrainApp {
    fn pre_park<'turn>(
        &mut self,
        _: schedule::Application<'turn, 'd>,
        _: &mut o3::cell::region::Token<'d>,
    ) {
        let _ = self;
    }

    fn shutdown(&mut self) {
        let _ = self;
    }

    fn progress(
        &self,
        _: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        dope_core::driver::schedule::Progress::Quiescent
    }
}

impl<'d, const ID: u8> Application<'d, ID> for YieldingApp {
    type Conn = ();
    type Wire = Identity;
    type Send = Shared;
    type Input = dope_manifold::receive::Borrowed;

    fn connection(&self) -> Self::Conn {}
}

impl<'d, const ID: u8> app::Receive<'d, ID> for YieldingApp {
    type Continuation = app::continuation::Complete;
}

impl<'d, const ID: u8> app::BorrowedReceive<'d, ID> for YieldingApp {
    fn chunk<O, R: Retainable>(
        &mut self,
        _connection: connection::Ctx<'_, 'd, ID, Identity, (), O>,
        _egress: dope_net::link::egress::Queue<'_, 'd, 32>,
        _chunk: R,
        _driver: &mut Context<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::Ok
    }
}

impl<'d, const ID: u8> app::Lifecycle<'d, ID> for YieldingApp {
    fn connected<O>(
        &mut self,
        _key: Id<'d, ID>,
        _peer: dope_core::io::socket::Addr,
        _connection: connection::Ctx<'_, 'd, ID, Identity, (), O>,
        _egress: dope_net::link::egress::Queue<'_, 'd, 32>,
        _driver: &mut Context<'_, 'd>,
    ) {
        self.connected.hit();
        self.pending.set(true);
    }

    fn sent(&mut self, _connection: connection::Id<'d, ID>, _has_pending_egress: bool) {}

    fn close<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Identity, (), O>,
        _egress: dope_net::link::egress::Queue<'_, 'd, 32>,
        reason: dope_manifold::connector::lifecycle::CloseReason,
        _driver: &mut Context<'_, 'd>,
    ) -> app::CloseOutcome {
        let _ = connection;
        if !self.yielded.replace(true) {
            app::CloseOutcome::Yield
        } else {
            self.resumed.set(true);
            self.closed.hit();
            app::CloseOutcome::Complete(reason)
        }
    }
}

impl<'d, const ID: u8> app::RequestSource<'d, ID> for YieldingApp {
    fn drain_requests(
        &self,
        _connection: connection::Id<'d, ID>,
        _state: &mut Self::Conn,
        _drain: &mut app::RequestDrain<'_, 'd, Self::Send>,
        _driver: &mut Context<'_, 'd>,
    ) -> Requests {
        if self.pending.replace(false) {
            Requests {
                close: Some(CloseKind::Permanent),
            }
        } else {
            Requests::default()
        }
    }
}

impl<'d, const ID: u8> app::Scheduling<'d, ID> for YieldingApp {
    fn pre_park<'turn>(
        &mut self,
        _: schedule::Application<'turn, 'd>,
        _: &mut o3::cell::region::Token<'d>,
    ) {
        let _ = self;
    }

    fn shutdown(&mut self) {
        let _ = self;
    }

    fn progress(
        &self,
        _: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        dope_core::driver::schedule::Progress::Quiescent
    }
}

/// `CloseKind::Reconnect` drops the socket and redials → a 2nd `connected()`.
#[test]
fn app_reconnect_close_redials() {
    let run = run(CloseKind::Reconnect);
    assert_eq!(run.connected.hits(), 2);
    assert_eq!(run.closed.hits(), 2);
    assert_eq!(run.accepted, 2);
}

/// `CloseKind::Permanent` retires exactly one connection. After its disconnect
/// callback, multiple complete backoff windows pass without a second accept.
#[test]
fn app_permanent_close_does_not_redial() {
    let run = run(CloseKind::Permanent);
    assert_eq!(run.connected.hits(), 1);
    assert_eq!(run.closed.hits(), 1);
    assert_eq!(run.accepted, 1);
}

#[test]
fn raw_close_yield_resumes_before_permanent_retirement() {
    let (addr, server) = Peer::hold(1);
    let connected = Gate::new();
    let closed = Gate::new();
    let resumed = Rc::new(Cell::new(false));

    AttemptConnector::new(addr).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        YieldingApp {
            connected: connected.clone(),
            closed: closed.clone(),
            pending: Cell::new(false),
            yielded: Cell::new(false),
            resumed: resumed.clone(),
        },
        |case| {
            case.until(&connected, 1);
            case.until(&closed, 1);
        },
    );

    server.join().expect("server join");
    assert!(resumed.get());
}

#[test]
fn exhausted_request_drain_is_resumed_through_the_typed_ready_target() {
    let (addr, server) = Peer::hold(1);
    let completed = Gate::new();
    let calls = Rc::new(Cell::new(0));
    let first_remaining = Rc::new(Cell::new(None));

    AttemptConnector::new(addr).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        BoundedDrainApp {
            completed: completed.clone(),
            remaining: Cell::new(schedule::MAX_TURN_WORK_BUDGET + 1),
            calls: calls.clone(),
            first_remaining: first_remaining.clone(),
        },
        |case| case.until(&completed, 1),
    );

    server.join().expect("server join");
    assert_eq!(first_remaining.get(), Some(1));
    assert!(calls.get() >= 2);
}
