use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    io::Read,
    net::{SocketAddr, TcpListener},
    rc::Rc,
    sync::mpsc,
    thread,
    time::Duration,
};

use dope_core::driver::{
    schedule::{self, ready},
    settings,
};
use dope_manifold::{
    connector::{
        auxiliary,
        codec::{Codec, Parse},
        connection,
        lifecycle::{CloseReason, Stateless},
        session::{Ctx, Retirement, Scheduling, Session, Target},
    },
    service,
    timing::Balanced,
};
use dope_net::link::egress::data::Inline;
use dope_runtime::executor::Executor;
use o3::cell::region;

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

struct AuxiliarySession<'d> {
    codec: NeedMore,
    auxiliary: Rc<AuxiliaryState<'d>>,
    connected: dope_test::fibers::Gate,
}

impl<'d> Session<'d> for AuxiliarySession<'d> {
    type Codec = NeedMore;
    type ConnState = Stateless;
    type Send = Inline<16>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, _peer: dope_core::io::socket::Addr, context: &mut Ctx<'_, 'd, Self>) {
        self.auxiliary.pending.set(Some(context.conn_id));
        if let Some(ready) = self.auxiliary.ready.get() {
            ready.wake();
        }
        self.connected.hit();
    }

    fn response<'input>(&mut self, _head: (), _context: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
    }
}

impl<'d> Retirement<'d> for AuxiliarySession<'d> {
    fn disconnect(&mut self, _context: &mut Ctx<'_, 'd, Self>, _reason: CloseReason) {}
}

impl<'d> Scheduling<'d> for AuxiliarySession<'d> {}

impl<'d> Target<'d, 0, 1> for AuxiliarySession<'d> {}

struct AuxiliaryState<'d> {
    pending: Cell<Option<connection::Id<'d, 0>>>,
    cancellation: Cell<Option<connection::Id<'d, 0>>>,
    cancel_on_submit: bool,
    ready: Cell<Option<ready::Target<'d>>>,
    completed: dope_test::fibers::Gate,
    outcomes: Rc<RefCell<Vec<Result<(), auxiliary::Error>>>>,
}

struct AuxiliaryControl<'d>(Rc<AuxiliaryState<'d>>);

impl<'d> auxiliary::Control<'d, Inline<16>> for AuxiliaryControl<'d> {
    fn start(&mut self, ready: ready::Target<'d>) {
        self.0.ready.set(Some(ready));
    }

    fn has_requests(&self) -> bool {
        self.0.pending.get().is_some()
    }

    fn take_request<'turn>(
        &mut self,
        _permit: schedule::ApplicationPermit<'turn, 'd>,
        _region: &mut region::Token<'d>,
    ) -> Option<auxiliary::Request<'d, Inline<16>>> {
        let target = self.0.pending.take()?;
        if self.0.cancel_on_submit {
            self.0.cancellation.set(Some(target));
        }
        let mut payload = Inline::new();
        payload.try_extend(b"cancel").expect("inline payload");
        Some(auxiliary::Request::new(target, payload))
    }

    fn has_cancellations(&self) -> bool {
        self.0.cancellation.get().is_some()
    }

    fn take_cancellation<'turn>(
        &mut self,
        _permit: schedule::MaintenancePermit<'turn, 'd>,
        _region: &mut region::Token<'d>,
    ) -> Option<connection::Id<'d, 0>> {
        self.0.cancellation.take()
    }

    fn complete(
        &mut self,
        _ticket: auxiliary::Ticket<'d>,
        result: Result<(), auxiliary::Error>,
        _region: &mut region::Token<'d>,
    ) {
        self.0.outcomes.borrow_mut().push(result);
        self.0.completed.hit();
    }

    fn stop(&mut self, _region: &mut region::Token<'d>) {
        self.0.pending.set(None);
        self.0.cancellation.set(None);
        self.0.ready.set(None);
    }
}

type TestEnv = dope_manifold::Bundle<dope_net::tcp::Tcp, dope_net::wire::Identity, Balanced>;

type Connector<'d> = service::connector::Connector<
    'd,
    0,
    1,
    AuxiliarySession<'d>,
    service::Fixed<SocketAddr, SocketAddr>,
    service::reconcile::Preserve,
    SocketAddr,
    service::observe::Ignore,
    TestEnv,
    16,
    auxiliary::Enabled<AuxiliaryControl<'d>>,
>;

#[test]
fn fresh_send_reuses_the_exact_peer_and_keeps_a_separate_slot() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("listener address");
    let (payload_tx, payload_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (main, _) = listener.accept().expect("session connection");
        let (mut auxiliary, _) = listener.accept().expect("auxiliary connection");
        let mut payload = [0; 6];
        auxiliary
            .read_exact(&mut payload)
            .expect("auxiliary payload");
        payload_tx.send(payload).expect("publish payload");
        release_rx.recv().expect("release server");
        drop(auxiliary);
        drop(main);
    });

    let connected = dope_test::fibers::Gate::new();
    let completed = dope_test::fibers::Gate::new();
    let outcomes = Rc::new(RefCell::new(Vec::new()));
    Executor::new(settings::Config::for_tcp_profile::<Balanced>(2).expect("driver config"))
        .expect("executor")
        .with_storage(())
        .enter(|mut session| {
            let backoff = session.hash_state(service::health::Domain::DEFAULT);
            let snapshot = service::Snapshot::try_new(
                service::Revision::new(1),
                [service::Endpoint::new(addr, addr)],
            )
            .expect("snapshot");
            let auxiliary = Rc::new(AuxiliaryState {
                pending: Cell::new(None),
                cancellation: Cell::new(None),
                cancel_on_submit: false,
                ready: Cell::new(None),
                completed: completed.clone(),
                outcomes: outcomes.clone(),
            });
            let connector = Connector::new_with_auxiliary(
                AuxiliarySession {
                    codec: NeedMore,
                    auxiliary: auxiliary.clone(),
                    connected: connected.clone(),
                },
                AuxiliaryControl(auxiliary),
                service::Fixed::new(snapshot),
                service::connector::Config::new(
                    1,
                    service::health::Backoff::new(Duration::from_millis(10), backoff)
                        .expect("backoff"),
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
                        dope_test::fibers::TEST.run_until(&mut app, &completed, 1);
                    },
                )
                .expect("application teardown");
        });

    assert_eq!(outcomes.borrow().as_slice(), &[Ok(())]);
    assert_eq!(payload_rx.recv().expect("payload"), *b"cancel");
    release_tx.send(()).expect("release");
    server.join().expect("server");
}

#[test]
fn explicit_cancellation_settles_the_mirrored_lane_without_scanning() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("listener address");
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (main, _) = listener.accept().expect("session connection");
        release_rx.recv().expect("release server");
        drop(main);
    });

    let connected = dope_test::fibers::Gate::new();
    let completed = dope_test::fibers::Gate::new();
    let outcomes = Rc::new(RefCell::new(Vec::new()));
    Executor::new(settings::Config::for_tcp_profile::<Balanced>(2).expect("driver config"))
        .expect("executor")
        .with_storage(())
        .enter(|mut session| {
            let backoff = session.hash_state(service::health::Domain::DEFAULT);
            let snapshot = service::Snapshot::try_new(
                service::Revision::new(1),
                [service::Endpoint::new(addr, addr)],
            )
            .expect("snapshot");
            let auxiliary = Rc::new(AuxiliaryState {
                pending: Cell::new(None),
                cancellation: Cell::new(None),
                cancel_on_submit: true,
                ready: Cell::new(None),
                completed: completed.clone(),
                outcomes: outcomes.clone(),
            });
            let connector = Connector::new_with_auxiliary(
                AuxiliarySession {
                    codec: NeedMore,
                    auxiliary: auxiliary.clone(),
                    connected: connected.clone(),
                },
                AuxiliaryControl(auxiliary),
                service::Fixed::new(snapshot),
                service::connector::Config::new(
                    1,
                    service::health::Backoff::new(Duration::from_millis(10), backoff)
                        .expect("backoff"),
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
                        dope_test::fibers::TEST.run_until(&mut app, &completed, 1);
                    },
                )
                .expect("application teardown");
        });

    assert_eq!(
        outcomes.borrow().as_slice(),
        &[Err(auxiliary::Error::Transport)]
    );
    release_tx.send(()).expect("release");
    server.join().expect("server");
}
