#![cfg(target_os = "linux")]

extern crate dope;

use std::cell::Cell;
use std::fs;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

use dope::manifold::env::Bundle;
use dope::manifold::listener::Listener;
use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::config::Config;
use dope::manifold::listener::state::EgressCtx;
use dope::manifold::typed::TypedToken;
use dope::manifold::{Manifold, Outcome, listener};
use dope::runtime::dispatcher::Idle;
use dope::runtime::executor::Executor;
use dope::runtime::launcher::WorkerContext;
use dope::runtime::profile::Throughput;
use dope_net::link::slot::Slot;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use dope_test::Harness;
use o3::buffer::RetainBytes;
use o3::cell::BrandCell;

const CHILD: &str = "DOPE_LISTENER_TEARDOWN_CHILD";
const MARKER: &str = "DOPE_LISTENER_TEARDOWN_MARKER";
const ACCEPTED: &str = "DOPE_LISTENER_TEARDOWN_ACCEPTED";

struct TeardownState {
    accepted: Cell<usize>,
    calls: Cell<usize>,
    marker: PathBuf,
    accepted_marker: PathBuf,
}

struct TeardownApp {
    state: TeardownState,
}

impl<'d> Application<'d> for TeardownApp {
    type Conn = ();
    type Wire = Identity;
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, TeardownApp> for TeardownApp {
    fn chunk<R: RetainBytes>(
        _app: Pin<&mut TeardownApp>,
        _slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        _chunk: R,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        Outcome::Ok
    }

    fn accept(
        app: Pin<&mut TeardownApp>,
        _slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let state = &app.get_mut().state;
        let accepted = state.accepted.get() + 1;
        state.accepted.set(accepted);
        fs::write(&state.accepted_marker, accepted.to_string()).expect("write accepted marker");
        Outcome::Ok
    }

    fn teardown(
        app: Pin<&mut TeardownApp>,
        _slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
    ) {
        let state = &app.get_mut().state;
        let calls = state.calls.get();
        state.calls.set(calls + 1);
        if calls == 0 {
            panic!("listener teardown panic");
        }
        fs::write(&state.marker, b"cleaned").expect("write marker");
    }
}

#[repr(transparent)]
struct DropLive<M> {
    inner: M,
}

impl<'d, M: Manifold<'d>> Manifold<'d> for DropLive<M> {
    const ID: u8 = M::ID;

    fn dispatch(
        self: Pin<&mut Self>,
        ev: dope::Event<'d>,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        M::dispatch(
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) },
            ev,
            driver,
        );
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        let target = target.retag::<'d, M>();
        M::activate(
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) },
            target,
            driver,
        );
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut dope::DriverContext<'_, 'd>) {
        M::pre_park(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, driver);
    }

    fn idle(self: Pin<&Self>, region: &o3::cell::RegionToken<'d>) -> Idle {
        M::idle(unsafe { self.map_unchecked(|s| &s.inner) }, region)
    }

    fn shutdown(self: Pin<&mut Self>, _driver: &mut dope::DriverContext<'_, 'd>) {
        let _ = self;
    }
}

type Env = Bundle<Tcp, Identity, Throughput>;
type TeardownListener<'d> = Listener<'d, 'd, 0, TeardownApp, Env>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<'d> {
    #[pin]
    #[manifold]
    listener: DropLive<TeardownListener<'d>>,
}

fn serve(state: TeardownState, cfg: Config<Tcp>, context: WorkerContext) -> std::io::Result<()> {
    let driver = dope::driver::Config::for_tcp_profile::<Throughput>(cfg.max_connections);
    Executor::with_seed(driver, context.seed())?
        .with_storage(dope_net::link::egress::storage::Storage::default())
        .enter(|mut session| {
            let egress = session.storage();
            context.try_register_shutdown(&mut session.driver_access())?;
            let hash_builder = session.seed().derive(dope::hash::domain::ACCEPT).state();
            let listener = Listener::<0, TeardownApp, Env>::open_in(
                TeardownApp { state },
                cfg,
                hash_builder,
                egress,
                &mut session.driver_access(),
            )?;
            let dispatcher = std::pin::pin!(BrandCell::new(Dispatcher {
                listener: DropLive { inner: listener },
            }));
            session.run(dispatcher.as_ref())
        })
}

fn wait_for_count(marker: &Path, expected: usize) {
    for _ in 0..200 {
        let accepted = fs::read_to_string(marker)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        if accepted >= expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let accepted = fs::read_to_string(marker)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    assert!(accepted >= expected);
}

#[test]
fn teardown_panic_aborts_after_cleaning_other_live_slots() {
    if std::env::var_os(CHILD).is_some() {
        let marker = PathBuf::from(std::env::var_os(MARKER).expect("marker path"));
        let accepted_marker =
            PathBuf::from(std::env::var_os(ACCEPTED).expect("accepted marker path"));
        let harness = Harness::bind().expect("bind ephemeral port");
        let bind = harness.addr();
        let server_marker = marker.clone();
        let server_accepted_marker = accepted_marker.clone();
        let _streams = harness.run(
            move |ctx| {
                serve(
                    TeardownState {
                        accepted: Cell::new(0),
                        calls: Cell::new(0),
                        marker: server_marker.clone(),
                        accepted_marker: server_accepted_marker.clone(),
                    },
                    Config::<Tcp> {
                        max_connections: 64,
                        bind,
                        backlog: 128,
                        stream: Default::default(),
                        transport: Default::default(),
                        egress: Default::default(),
                    },
                    ctx,
                )
            },
            |addr| {
                let streams = [
                    TcpStream::connect(addr).expect("first connect"),
                    TcpStream::connect(addr).expect("second connect"),
                ];
                wait_for_count(&accepted_marker, 3);
                streams
            },
        );
        panic!("listener teardown did not abort");
    }

    let marker = std::env::temp_dir().join(format!(
        "dope-listener-teardown-{}.marker",
        std::process::id()
    ));
    let accepted_marker = std::env::temp_dir().join(format!(
        "dope-listener-teardown-{}.accepted",
        std::process::id()
    ));
    let _ = fs::remove_file(&marker);
    let _ = fs::remove_file(&accepted_marker);
    let status = dope_test::respawn_self(
        "teardown_panic_aborts_after_cleaning_other_live_slots",
        &[
            (CHILD, "1"),
            (MARKER, marker.to_str().expect("marker str")),
            (
                ACCEPTED,
                accepted_marker.to_str().expect("accepted marker str"),
            ),
        ],
    );

    dope_test::expect_abort(status);
    assert_eq!(fs::read(&marker).expect("read marker"), b"cleaned");
    fs::remove_file(marker).expect("remove marker");
    fs::remove_file(accepted_marker).expect("remove accepted marker");
}
