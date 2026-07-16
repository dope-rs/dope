#![allow(dead_code, unused_imports)]

pub mod alloc;

use std::cell::Cell;
use std::net::{SocketAddr, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use dope::driver::profile::DriverProfile;
use dope::driver::ready::ReadySlot;
use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::manifold::file::{Files, FilesFactory};
use dope::runtime::profile;
use dope::runtime::{Dispatcher, Executor, Session};
use dope::{Completion, Cqe, DriverContext, DriverRef, Event, driver};
use dope_fiber::{Context, Fiber, ListenerPort, ListenerPortFactory, OneShot, SessionExt};
use o3::cell::BrandCell;

pub const GUARD: Duration = Duration::from_secs(5);

pub fn throughput_cfg() -> driver::Config {
    driver::Config::for_profile::<profile::Throughput>()
}

pub fn exec_for<P: DriverProfile>() -> Executor<()> {
    Executor::new(driver::Config::for_profile::<P>()).expect("executor")
}

pub fn exec() -> Executor<()> {
    exec_for::<profile::Throughput>()
}

pub fn with_session<R>(f: impl for<'scope, 'd> FnOnce(Session<'scope, 'd>) -> R) -> R {
    exec().enter(f)
}

pub fn with_session_for<P: DriverProfile, R>(
    f: impl for<'scope, 'd> FnOnce(Session<'scope, 'd>) -> R,
) -> R {
    exec_for::<P>().enter(f)
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

pub fn tok(idx: u32) -> Token {
    Token::new(0, SlotIndex::new(idx), Epoch::INITIAL)
}

pub fn drain_tokens(driver: DriverRef<'_>) -> Vec<Token> {
    let mut out = Vec::new();
    driver.drain_ready(|token| out.push(token));
    out
}

pub fn with_poll_context<R>(f: impl for<'poll, 'd> FnOnce(Pin<&mut Context<'poll, 'd>>) -> R) -> R {
    with_context(f)
}

pub fn with_context<R>(run: impl for<'poll, 'd> FnOnce(Pin<&mut Context<'poll, 'd>>) -> R) -> R {
    with_session(|mut session| {
        let slot = pin!(session.driver().make_ready_slot(tok(0)));
        let reference = session.driver();
        let access = session.driver_access();
        let mut context = pin!(Context::from_ready(reference, slot.as_ref().key(), access));
        run(context.as_mut())
    })
}

pub fn poll_with_slot<'scope, 'd, S, F>(
    session: &mut Session<'scope, 'd, S>,
    slot: Pin<&ReadySlot<'d>>,
    fiber: Pin<&mut F>,
) -> Poll<F::Output>
where
    F: Fiber<'d>,
{
    let reference = session.driver();
    let access = session.driver_access();
    let mut context = pin!(Context::from_ready(reference, slot.key(), access));
    Fiber::poll(fiber, context.as_mut())
}

pub fn poll_ready<'d, F: Fiber<'d> + ?Sized>(
    fiber: Pin<&mut F>,
    cx: Pin<&mut Context<'_, 'd>>,
) -> F::Output {
    match Fiber::poll(fiber, cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("expected Ready"),
    }
}

pub fn drive<'scope, 'd, S, D: Dispatcher<'d>, F: Fiber<'d>>(
    sess: &mut Session<'scope, 'd, S>,
    app: Pin<&BrandCell<'d, D>>,
    fiber: F,
) -> F::Output {
    sess.block_on(app, fiber).expect("runtime park")
}

pub fn assert_unwinds<R>(f: impl FnOnce() -> R) {
    assert!(catch_unwind(AssertUnwindSafe(f)).is_err());
}

pub fn counter() -> Rc<Cell<usize>> {
    Rc::new(Cell::new(0))
}

pub fn reserve_addr() -> SocketAddr {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve address");
    socket.local_addr().expect("local address")
}

pub fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect_timeout(&addr, GUARD).expect("connect")
}

pub fn connect_with_read_timeout(addr: SocketAddr, timeout: Duration) -> TcpStream {
    let stream = connect(addr);
    stream
        .set_read_timeout(Some(timeout))
        .expect("read timeout");
    stream
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

pub struct TempFile(String);

impl TempFile {
    pub fn with(tag: &str, contents: &[u8]) -> Self {
        let path = std::env::temp_dir()
            .join(format!("dope_test_{}_{}", std::process::id(), tag))
            .to_string_lossy()
            .into_owned();
        std::fs::write(&path, contents).expect("write temp");
        Self(path)
    }

    pub fn path(&self) -> &std::path::Path {
        std::path::Path::new(&self.0)
    }

    pub fn path_str(&self) -> &str {
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub fn pump_events<'scope, 'd, S>(
    sess: &mut Session<'scope, 'd, S>,
    mut handle: impl FnMut(Event),
    mut done: impl FnMut() -> bool,
) -> bool {
    let mut buf = [Cqe::ZERO; 32];
    for _ in 0..500 {
        if done() {
            return true;
        }
        let mut driver = sess.driver_access();
        let _ = driver.wait(Some(Duration::from_millis(5)));
        let n = driver.drain(&mut buf);
        for cqe in &buf[..n] {
            if let Ok(event) = unsafe { Event::from_cqe(*cqe) } {
                handle(event);
            }
        }
    }
    done()
}

pub fn drop_pending<'d, F: Fiber<'d>>(driver: &mut DriverContext<'_, 'd>, op: F, tag: u8) {
    let mut one = pin!(OneShot::new(op, tag, driver.driver_ref()));
    one.as_mut().pre_park(driver);
    assert!(!one.as_ref().is_done());
}
