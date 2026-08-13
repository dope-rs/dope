use std::{
    cell::Cell,
    fs,
    net::TcpStream,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use dope_core::driver::settings;
use dope_manifold::{
    Bundle, Outcome,
    listener::{Listener, config::Config, connection, handler::Application},
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_test::Harness;
use o3::buffer::bytes::Retainable;

mod sealed;

const CHILD: &str = "DOPE_LISTENER_TEARDOWN_CHILD";
const MARKER: &str = "DOPE_LISTENER_TEARDOWN_MARKER";
const ACCEPTED: &str = "DOPE_LISTENER_TEARDOWN_ACCEPTED";

struct TeardownState {
    accepted: Cell<usize>,
    calls: Cell<usize>,
    marker: PathBuf,
    accepted_marker: PathBuf,
    dropping: std::rc::Rc<Cell<bool>>,
}

struct TeardownApp {
    state: TeardownState,
}

impl<'d, const ID: u8> Application<'d, ID> for TeardownApp {
    type Conn = ();
    type Wire = Identity;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let state = &self.get_mut().state;
        let accepted = state.accepted.get() + 1;
        state.accepted.set(accepted);
        fs::write(&state.accepted_marker, accepted.to_string()).expect("write accepted marker");
        Outcome::Ok
    }

    fn close(self: Pin<&mut Self>, _connection: connection::Ctx<'_, 'd, ID, Identity, ()>) {
        let state = &self.get_mut().state;
        if !state.dropping.get() {
            return;
        }
        let calls = state.calls.get();
        state.calls.set(calls + 1);
        if calls == 0 {
            panic!("listener teardown panic");
        }
        fs::write(&state.marker, b"cleaned").expect("write marker");
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for TeardownApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _chunk: R,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        Outcome::Ok
    }
}

#[pin_project::pin_project(PinnedDrop)]
struct DropLive<M> {
    #[pin]
    inner: M,
    stopping: bool,
    dropping: std::rc::Rc<Cell<bool>>,
}

#[pin_project::pinned_drop]
impl<M> PinnedDrop for DropLive<M> {
    fn drop(self: Pin<&mut Self>) {
        self.project().dropping.set(true);
    }
}

type Env = Bundle<Tcp, Identity, Throughput>;
type TeardownListener<'d> = Listener<'d, 0, TeardownApp, Env>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(
    core = dope_core,
    manifold = dope_manifold,
    runtime = dope_runtime,
    region = o3::cell::region::Token,
)]
struct Dispatcher<'d> {
    #[pin]
    #[manifold]
    listener: DropLive<TeardownListener<'d>>,
}

fn serve(
    state: TeardownState,
    cfg: Config<Tcp>,
    source: dope_runtime::shutdown::Source,
) -> std::io::Result<dope_runtime::shutdown::Requested> {
    let driver = settings::Config::for_tcp_profile::<Throughput>(cfg.max_connections)?;
    dope_runtime::executor::Executor::new(driver)?
        .with_shutdown(source)?
        .enter(|mut session| {
            let dropping = state.dropping.clone();
            let hash_builder = session.hash_state(dope_manifold::listener::Domain::DEFAULT);
            let listener = Listener::<0, TeardownApp, Env>::open_in(
                TeardownApp { state },
                cfg,
                hash_builder,
                &mut session.driver_access(),
            )?;
            session.with_app(
                Dispatcher {
                    listener: DropLive {
                        inner: listener,
                        stopping: false,
                        dropping,
                    },
                },
                |mut app| app.run(),
            )?
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
fn unquiesced_retained_listener_aborts_before_owner_drop() {
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
                let dropping = std::rc::Rc::new(Cell::new(false));
                serve(
                    TeardownState {
                        accepted: Cell::new(0),
                        calls: Cell::new(0),
                        marker: server_marker.clone(),
                        accepted_marker: server_accepted_marker.clone(),
                        dropping,
                    },
                    Config::<Tcp> {
                        max_connections: 64,
                        direct_flights: 0,
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
        panic!("unquiesced retained listener teardown did not abort");
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
    let process = dope_test::checks::panics::Process::respawn(
        "teardown::unquiesced_retained_listener_aborts_before_owner_drop",
        &[
            (CHILD, "1"),
            (MARKER, marker.to_str().expect("marker str")),
            (
                ACCEPTED,
                accepted_marker.to_str().expect("accepted marker str"),
            ),
        ],
    );

    process.expect_abort();
    assert!(
        !marker.exists(),
        "an unquiesced retained owner must not be dropped"
    );
    fs::remove_file(accepted_marker).expect("remove accepted marker");
}
