use std::{io, pin};

use dope::{
    core::{
        driver::{self, settings, storage},
        io::socket,
    },
    manifold::{self, timing::Balanced},
    net::{self, wire::Identity},
    runtime::{executor::Executor, random},
};
use dope_fiber::{
    abi,
    net::{
        connector::{Connector, Handle, Port},
        server::{Listener, ListenerPort},
    },
};
use dope_test::fibers;

struct MoveOnlyAddr;

struct MoveOnlyTransport;

type MoveOnlyConnector<'scope, 'd> = Connector<'scope, 'd, 0, MoveOnlyTransport, Identity>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct MoveOnlyApp<'d, 'scope> {
    #[pin]
    #[manifold]
    connector: MoveOnlyConnector<'scope, 'd>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl net::Transport for MoveOnlyTransport {
    type Addr = MoveOnlyAddr;
    type StreamConfig = ();

    fn to_sock_addr(_: &Self::Addr) -> io::Result<socket::Addr> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn stream_options(_: Self::StreamConfig) -> io::Result<socket::option::StreamOptions> {
        Ok(socket::option::StreamOptions::default())
    }
}

impl net::ListenerTransport for MoveOnlyTransport {
    type ListenerConfig = ();

    fn bind_listener_slot<'d>(
        _: &mut driver::Context<'_, 'd>,
        _: &Self::Addr,
        _: i32,
        _: &Self::ListenerConfig,
    ) -> io::Result<(
        dope::core::io::fd::handles::Descriptor<'d>,
        std::net::SocketAddr,
    )> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn per_ip_limit(_: &Self::ListenerConfig) -> Option<u32> {
        None
    }
}

fn connect_is_a_fiber<'scope, 'd>(
    handle: Handle<'scope, 'd, MoveOnlyTransport, Identity>,
    addr: MoveOnlyAddr,
) {
    fn require_fiber<'d>(_: &impl abi::Fiber<'d>) {}

    let connect = handle.connect(addr, ());
    require_fiber(&connect);
}

fn bind_move_only_listener<'scope, 'd>(
    port: &'scope ListenerPort<'d, Identity>,
    driver: &mut driver::Context<'_, 'd>,
    addr: MoveOnlyAddr,
    hash_builder: random::HashState<'d>,
) -> io::Result<Listener<'scope, 'd, 0, MoveOnlyTransport, Identity>> {
    Listener::bind(port, driver, addr, 1, (), (), hash_builder)
}

fn move_only_listener_is_a_manifold<'scope, 'd>(
    listener: pin::Pin<&mut Listener<'scope, 'd, 0, MoveOnlyTransport, Identity>>,
) {
    fn require<'d>(_: pin::Pin<&mut impl manifold::dispatch::raw::Manifold<'d>>) {}

    require(listener);
}

#[test]
fn connector_accepts_move_only_addresses() {
    fn require_factory(_: impl storage::Factory) {}

    let factory = Port::<MoveOnlyTransport, Identity>::factory(1).unwrap();
    require_factory(factory);
    let _ = connect_is_a_fiber;
}

#[test]
fn failed_target_wakes_each_waiter_and_releases_attempt_capacity() {
    let config = settings::Config::for_tcp_profile::<Balanced>(1).expect("driver config");
    Executor::new(config)
        .expect("executor")
        .with_factory(Port::<MoveOnlyTransport, Identity>::factory(1).expect("connector capacity"))
        .try_enter(|mut session| {
            let storage = session.storage();
            let mut driver = session.driver_access();
            let connector = storage.connector(&mut driver).expect("connector");
            let handle = session.storage().handle();
            let dispatcher = MoveOnlyApp {
                connector,
                driver: ::core::marker::PhantomData,
            };

            session
                .with_app(dispatcher, |mut app| {
                    for _ in 0..2 {
                        let result = fibers::TEST.drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                                handle.connect(MoveOnlyAddr, ()).await
                            }),
                        );
                        let Err(error) = result else {
                            panic!("unsupported target unexpectedly connected");
                        };
                        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                    }
                })
                .expect("application teardown");
        })
        .expect("connector route");
}

#[test]
fn listener_accepts_move_only_addresses() {
    let _ = bind_move_only_listener;
    let _ = move_only_listener_is_a_manifold;
}
