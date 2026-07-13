#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::net::{SocketAddr, TcpStream};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use dope::fiber::{Fiber, Holding, Sleep};
use dope::manifold::env::Bundle;
use dope::manifold::listener::config::Config;
use dope::manifold::timer::Timer;
use dope::runtime::profile;
use dope::transport::Tcp;
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::wire::Identity;
use dope::{Dispatcher, DriverCfg, DriverConfig, Executor, Session, WakeRef, WakerSet};

/// Free of every manifold under test, and of `block_on`'s reserved 255.
pub const GUARD_ID: u8 = 254;

pub const GUARD: Duration = Duration::from_secs(5);

pub type Guard = Timer<GUARD_ID>;

pub type Wired<W> = Bundle<Tcp, W, profile::Throughput>;

pub type Plain = Wired<Identity>;

pub type TcpConfig = Config<Tcp>;

/// An executor and a `Config` that agree on `max_conn`.
///
/// ```ignore
/// let (mut exec, cfg) = common::tcp_host(64, ListenerOpts::default());
/// let listener = Listener::<0, App, Plain>::open_in(app, cfg, sess.driver())?;
/// ```
pub fn tcp_host(max_conn: usize, listener_opts: ListenerOpts) -> (Executor, TcpConfig) {
    let cfg = <DriverCfg as DriverConfig>::for_tcp_profile::<profile::Throughput>(max_conn);
    let exec = Executor::new(cfg).expect("executor");
    let config = Config {
        max_conn,
        bind: "127.0.0.1:0".parse::<SocketAddr>().expect("parse"),
        backlog: 128,
        stream_opts: Default::default(),
        listener_opts,
    };
    (exec, config)
}

/// Counts runtime-side events and wakes the driving fiber on each one.
///
/// ```ignore
/// fn on_close(&mut self, ..) { self.gate.hit() }
/// ...
/// run_until(&mut exec, app.as_mut(), guard, &gate, 1);
/// ```
///
/// Nothing here polls a clock. With no waker pending the driver parks for real,
/// so `pre_park` and `idle` stay under test — a `wake_by_ref()` spin pins the
/// park timeout at zero and hides them.
#[derive(Default)]
pub struct Gate {
    hits: Cell<u32>,
    wakers: RefCell<WakerSet>,
}

impl Gate {
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    pub fn hit(&self) {
        self.hits.set(self.hits.get() + 1);
        self.wakers.borrow_mut().drain_wake();
    }

    pub fn hits(&self) -> u32 {
        self.hits.get()
    }
}

struct Until<'d> {
    gate: Rc<Gate>,
    want: u32,
    guard: Pin<Box<Fiber<'d, Sleep<'d, GUARD_ID>>>>,
}

impl Future for Until<'_> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        let this = self.get_mut();
        if this.gate.hits() >= this.want {
            return Poll::Ready(true);
        }
        if this.guard.as_mut().poll(cx).is_ready() {
            return Poll::Ready(false);
        }
        this.gate
            .wakers
            .borrow_mut()
            .register(WakeRef::verified(cx.waker()));
        Poll::Pending
    }
}

/// Drive `app` until `gate` takes `want` hits. [`GUARD`] bounds a hang; it never
/// ends the success path.
pub fn run_until<'d, D: Dispatcher<'d>>(
    sess: &mut Session<'d>,
    app: Pin<&mut D>,
    guard: Holding<'_, Guard>,
    gate: &Rc<Gate>,
    want: u32,
) {
    let until = Until {
        gate: gate.clone(),
        want,
        guard: Box::pin(guard.sleep(GUARD)),
    };
    let reached = dope_extra::block_on(sess, app, Fiber::new(until));
    assert!(
        reached,
        "timed out after {GUARD:?}: gate took {} of {want} hits",
        gate.hits()
    );
}

/// `open_in` has already bound and listened, so the backlog takes the SYN
/// whether or not the executor is running yet.
pub fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect_timeout(&addr, GUARD).expect("connect")
}
