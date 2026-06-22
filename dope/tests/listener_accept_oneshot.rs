#![cfg(target_os = "linux")]

use std::cell::{Cell, RefCell};
use std::io::Read;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::time::Duration;

use dope::fiber::Fiber;
use dope::manifold::Outcome;
use dope::manifold::env::Bundle;
use dope::manifold::listener::config::Config;
use dope::manifold::listener::{self, Application, Listener};
use dope::runtime::profile;
use dope::transport::Tcp;
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk};
use dope::{Driver, DriverConfig, Executor};

struct AcceptTrace {
    count: Cell<u32>,
    peer_ips: RefCell<Vec<Option<IpAddr>>>,
}

impl AcceptTrace {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            count: Cell::new(0),
            peer_ips: RefCell::new(Vec::new()),
        })
    }

    fn record(&self, peer_ip: Option<IpAddr>) {
        self.count.set(self.count.get() + 1);
        self.peer_ips.borrow_mut().push(peer_ip);
    }
}

struct TraceApp {
    trace: Rc<AcceptTrace>,
}

impl Application for TraceApp {
    type Conn = ();
    type Wire = Identity;

    fn on_chunk(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) -> Outcome {
        Outcome::Ok
    }

    fn on_send(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) {
    }

    fn on_accept(
        &mut self,
        slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) -> Outcome {
        self.trace.record(slot.state.peer_ip());
        Outcome::Ok
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App {
    #[pin]
    #[manifold]
    listener: Listener<0, TraceApp, Bundle<Tcp, Identity, profile::Throughput>>,
}

fn make_exec(max_conn: usize) -> Executor {
    let cfg = <dope::DriverCfg as DriverConfig>::for_tcp_profile::<profile::Throughput>(max_conn);
    Executor::new(cfg).expect("executor")
}

fn build_listener(
    drv: &mut Driver,
    max_conn: usize,
    per_ip_cap: u32,
    trace: Rc<AcceptTrace>,
) -> (
    Listener<0, TraceApp, Bundle<Tcp, Identity, profile::Throughput>>,
    SocketAddr,
) {
    let bind: SocketAddr = "127.0.0.1:0".parse().expect("parse");
    let listener_opts = ListenerOpts {
        per_ip_cap: Some(per_ip_cap),
        ..ListenerOpts::default()
    };
    let cfg = Config::<Tcp> {
        max_conn,
        bind,
        backlog: 128,
        stream_opts: Default::default(),
        listener_opts,
    };
    let listener = Listener::<0, TraceApp, Bundle<Tcp, Identity, profile::Throughput>>::open_in(
        TraceApp { trace },
        cfg,
        drv,
    )
    .expect("open_in");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

fn wait_for_addr(addr: SocketAddr) -> TcpStream {
    for _ in 0..200 {
        if let Ok(s) = TcpStream::connect_timeout(&addr, Duration::from_millis(50)) {
            return s;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("could not connect to {addr}");
}

fn drive_until<F: FnMut() -> bool + 'static>(exec: &mut Executor, app: Pin<&mut App>, mut done: F) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let fiber = Fiber::new(std::future::poll_fn(move |cx| {
        if done() || std::time::Instant::now() >= deadline {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }));
    dope_extra::block_on(exec, app, fiber);
}

#[test]
fn test_oneshot_accept_basic() {
    let trace = AcceptTrace::new();
    let mut exec = make_exec(64);
    let drv = exec.driver_mut();
    let (listener, addr) = build_listener(drv, 64, 0, trace.clone());
    let mut app = pin!(App { listener });

    let client_trace = trace.clone();
    let handle = std::thread::spawn(move || {
        let mut s = wait_for_addr(addr);
        s.set_read_timeout(Some(Duration::from_secs(1))).ok();
        let mut buf = [0u8; 1];
        let _ = s.read(&mut buf);
        drop(s);
        let _ = client_trace;
    });

    let trace_for_done = trace.clone();
    drive_until(&mut exec, app.as_mut(), move || {
        trace_for_done.count.get() >= 1
    });

    handle.join().expect("client join");
    assert_eq!(
        trace.count.get(),
        1,
        "accept Application::on_accept must fire exactly once"
    );
}

#[test]
fn test_oneshot_accept_peer_addr() {
    let trace = AcceptTrace::new();
    let mut exec = make_exec(64);
    let drv = exec.driver_mut();
    let (listener, addr) = build_listener(drv, 64, 0, trace.clone());
    let mut app = pin!(App { listener });

    let handle = std::thread::spawn(move || {
        let mut s = wait_for_addr(addr);
        s.set_read_timeout(Some(Duration::from_secs(1))).ok();
        let mut buf = [0u8; 1];
        let _ = s.read(&mut buf);
        drop(s);
    });

    let trace_for_done = trace.clone();
    drive_until(&mut exec, app.as_mut(), move || {
        trace_for_done.count.get() >= 1
    });

    handle.join().expect("client join");
    assert_eq!(trace.count.get(), 1, "exactly one accept");
    let ips = trace.peer_ips.borrow();
    assert_eq!(ips.len(), 1, "peer_ips populated");
    let ip = ips[0].expect("peer_ip recorded with sockaddr active");
    let want: IpAddr = "127.0.0.1".parse().expect("parse");
    assert_eq!(ip, want, "peer_ip must be loopback");
}

#[test]
fn test_oneshot_accept_per_ip_cap() {
    let trace = AcceptTrace::new();
    let mut exec = make_exec(64);
    let drv = exec.driver_mut();
    let (listener, addr) = build_listener(drv, 64, 2, trace.clone());
    let mut app = pin!(App { listener });

    let n_clients = 3usize;
    let mut handles = Vec::with_capacity(n_clients);
    for _ in 0..n_clients {
        handles.push(std::thread::spawn(move || {
            if let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_millis(500)) {
                s.set_read_timeout(Some(Duration::from_secs(2))).ok();
                let mut buf = [0u8; 1];
                let _ = s.read(&mut buf);
                drop(s);
            }
        }));
    }

    let trace_for_done = trace.clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let fiber = Fiber::new(std::future::poll_fn(move |cx| {
        if std::time::Instant::now() >= deadline {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }));
    dope_extra::block_on(&mut exec, app.as_mut(), fiber);

    for h in handles {
        let _ = h.join();
    }
    let accepted = trace.count.get();
    assert_eq!(
        accepted, 2,
        "per_ip_cap=2 must cap loopback connections at 2, got {accepted}"
    );
    let _ = trace_for_done;
}
