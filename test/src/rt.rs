use std::net::SocketAddr;
use std::pin::pin;

use dope::driver::profile::DriverProfile;
use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::manifold::env::{Bundle, Env};
use dope::manifold::file::{Files, FilesFactory};
use dope::manifold::listener::{Application, Config, Listener};
use dope::runtime::profile;
use dope::runtime::{Executor, Session};
use dope::{Driver, DriverContext, DriverRef, driver};
use dope_core::driver::ext::DriverExt;
use dope_fiber::{ListenerPort, ListenerPortFactory};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;

pub type Wired<W> = Bundle<Tcp, W, profile::Throughput>;

pub type Plain = Wired<Identity>;

pub type TcpConfig = Config<Tcp>;

pub fn throughput_cfg() -> driver::Config {
    driver::Config::for_profile::<profile::Throughput>()
}

pub fn exec_for<P: DriverProfile>() -> Executor<()> {
    Executor::new(driver::Config::for_profile::<P>()).expect("executor")
}

pub fn exec() -> Executor<()> {
    exec_for::<profile::Throughput>()
}

pub fn quic_exec(buf_entries: u32, buf_len: u32) -> Executor<()> {
    Executor::new(driver::Config::for_quic_udp(buf_entries, buf_len)).expect("executor")
}

pub fn with_session<R>(f: impl for<'scope, 'd> FnOnce(Session<'scope, 'd>) -> R) -> R {
    exec().enter(f)
}

pub fn with_session_for<P: DriverProfile, R>(
    f: impl for<'scope, 'd> FnOnce(Session<'scope, 'd>) -> R,
) -> R {
    exec_for::<P>().enter(f)
}

pub fn with_driver<R>(f: impl for<'d> FnOnce(DriverContext<'d, 'd>) -> R) -> R {
    let driver = Driver::new(driver::Config::for_quic_udp(1, 8)).expect("driver");
    let mut driver = pin!(driver);
    driver.as_mut().scope(|access, _token| f(access))
}

pub fn file_exec<const ID: u8, const N: usize>() -> Executor<FilesFactory<ID, N>> {
    Executor::new(throughput_cfg())
        .expect("executor")
        .with_storage_factory(Files::<ID, N>::factory())
}

pub fn listener_exec<P: DriverProfile>(
    max_connections: usize,
    tweak: impl FnOnce(driver::Config) -> driver::Config,
) -> Executor<ListenerPortFactory> {
    Executor::new(tweak(driver::Config::for_tcp_profile::<P>(max_connections)))
        .expect("executor")
        .with_storage_factory(ListenerPort::factory(max_connections))
}

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

pub fn tok(idx: u32) -> Token {
    Token::new(0, SlotIndex::new(idx), Epoch::INITIAL)
}

pub fn drain_tokens(driver: DriverRef<'_>) -> Vec<Token> {
    let mut out = Vec::new();
    driver.drain_ready(|token| out.push(token));
    out
}
