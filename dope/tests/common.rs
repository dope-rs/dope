#![allow(dead_code, unused_imports)]

use std::net::{SocketAddr, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::process::ExitStatus;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};
use std::{
    cell::Cell,
    io::{Read, Write},
};

use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::manifold::env::{Bundle, Env};
use dope::manifold::listener::{Application, Config, Listener};
use dope::runtime::profile;
use dope::runtime::{Dispatcher, Executor, Session};
use dope::{DriverContext, driver};
use dope_fiber::{Context, Fiber, SessionExt};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use o3::cell::BrandCell;

pub const GUARD: Duration = Duration::from_secs(5);

pub type Wired<W> = Bundle<Tcp, W, profile::Throughput>;

pub type Plain = Wired<Identity>;

pub type TcpConfig = Config<Tcp>;

pub fn tcp_host(
    max_connections: usize,
    listener_config: dope_net::tcp::listener::Config,
) -> (Executor<()>, TcpConfig) {
    let cfg = driver::Config::for_tcp_profile::<profile::Throughput>(max_connections);
    let exec = Executor::new(cfg).expect("executor");
    let config = Config {
        max_connections,
        bind: "127.0.0.1:0".parse::<SocketAddr>().expect("parse"),
        backlog: 128,
        stream: Default::default(),
        transport: listener_config,
        egress: Default::default(),
    };
    (exec, config)
}

pub struct Gate {
    hits: Cell<u32>,
}

impl Default for Gate {
    fn default() -> Self {
        Self { hits: Cell::new(0) }
    }
}

impl Gate {
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    pub fn hit(&self) {
        self.hits.set(self.hits.get() + 1);
    }

    pub fn hits(&self) -> u32 {
        self.hits.get()
    }
}

struct Until {
    gate: Rc<Gate>,
    want: u32,
    start: Instant,
}

impl<'d> Fiber<'d> for Until {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<bool> {
        let this = self.get_mut();
        if this.gate.hits() >= this.want {
            return Poll::Ready(true);
        }
        if this.start.elapsed() >= GUARD {
            return Poll::Ready(false);
        }
        cx.wake();
        Poll::Pending
    }
}

pub fn run_until<'scope, 'd, D: Dispatcher<'d>>(
    sess: &mut Session<'scope, 'd>,
    app: Pin<&BrandCell<'d, D>>,
    gate: &Rc<Gate>,
    want: u32,
) {
    let until = Until {
        gate: gate.clone(),
        want,
        start: Instant::now(),
    };
    let reached = sess.block_on(app, until).expect("runtime park");
    assert!(
        reached,
        "timed out after {GUARD:?}: gate took {} of {want} hits",
        gate.hits()
    );
}

pub fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect_timeout(&addr, GUARD).expect("connect")
}

pub fn exec() -> Executor<()> {
    Executor::new(driver::Config::for_profile::<profile::Throughput>()).expect("executor")
}

pub fn with_session<R>(f: impl for<'scope, 'd> FnOnce(Session<'scope, 'd>) -> R) -> R {
    exec().enter(f)
}

pub fn quic_exec(buf_entries: u32, buf_len: u32) -> Executor<()> {
    Executor::new(driver::Config::for_quic_udp(buf_entries, buf_len)).expect("executor")
}

pub fn tok(idx: u32) -> Token {
    Token::new(0, SlotIndex::new(idx), Epoch::INITIAL)
}

pub fn assert_unwinds<R>(f: impl FnOnce() -> R) {
    assert!(catch_unwind(AssertUnwindSafe(f)).is_err());
}

pub fn respawn_self(test_name: &str, envs: &[(&str, &str)]) -> ExitStatus {
    let mut cmd = std::process::Command::new(std::env::current_exe().expect("current test binary"));
    cmd.arg("--exact").arg(test_name);
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.status().expect("respawn self")
}

pub fn expect_abort(status: ExitStatus) {
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGABRT));
    }
}

pub fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

pub fn read_all(stream: &mut TcpStream) -> Vec<u8> {
    let mut got = Vec::new();
    stream.read_to_end(&mut got).expect("read to eof");
    got
}

pub fn spawn_peer<T: Send + 'static>(
    addr: SocketAddr,
    script: impl FnOnce(&mut TcpStream) -> T + Send + 'static,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(move || {
        let mut stream = connect(addr);
        script(&mut stream)
    })
}

pub fn request_reply(addr: SocketAddr, request: Vec<u8>) -> std::thread::JoinHandle<Vec<u8>> {
    spawn_peer(addr, move |stream| {
        stream.write_all(&request).expect("write request");
        read_all(stream)
    })
}

pub fn open_listener<'d, const ID: u8, A, E>(
    app: A,
    cfg: Config<E::Transport>,
    hash_builder: dope::hash::State,
    driver: &mut DriverContext<'_, 'd>,
) -> (Listener<'d, ID, A, E>, SocketAddr)
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
    E::Transport: 'static,
{
    let listener = Listener::open_in(app, cfg, hash_builder, driver).expect("listener");
    let addr = listener.local_addr().expect("local addr");
    (listener, addr)
}
