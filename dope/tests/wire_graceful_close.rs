use std::cell::Cell;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::time::Duration;

use dope::fiber::Fiber;
use dope::manifold::Outcome;
use dope::manifold::env::Bundle;
use dope::manifold::listener::config::Config;
use dope::manifold::listener::{self, Application, Listener};
use dope::runtime::profile;
use dope::token::Token;
use dope::transport::Tcp;
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::link::{Core, Slot};
use dope::transport::wire::{Reclaim, RecvChunk, Vectored, Wire};
use dope::{Driver, DriverConfig, Executor};

const BYE: &[u8] = b"<<BYE>>";

struct GracefulWire;

impl Wire for GracefulWire {
    type InitConfig = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn new(_: &()) -> Self {
        GracefulWire
    }

    fn process_recv<'a>(&mut self, bytes: &'a [u8]) -> Option<RecvChunk<'a>> {
        Some(RecvChunk::Borrowed(bytes))
    }

    fn submit_send(&mut self, core: &mut Core, plain: &[u8], ud: Token, driver: &mut Driver) -> usize {
        if plain.is_empty() {
            return 0;
        }
        core.submit_single(ud, plain, driver);
        plain.len()
    }

    fn submit_send_vectored(
        &mut self,
        core: &mut Core,
        vectored: Vectored<'_>,
        ud: Token,
        driver: &mut Driver,
    ) -> usize {
        if vectored.iovs.is_empty() {
            return 0;
        }
        let consumed: usize = vectored.iovs.iter().map(|v| v.len()).sum();
        core.submit_vectored(ud, vectored, driver);
        consumed
    }

    fn after_send_cqe(&mut self, _core: &mut Core, _n: usize, _ud: Token, _driver: &mut Driver) -> bool {
        false
    }

    fn flush_pending(&mut self, _core: &mut Core, _ud: Token, _driver: &mut Driver) {}

    fn on_graceful_close(&mut self, core: &mut Core, ud: Token, driver: &mut Driver) {
        core.submit_single(ud, BYE, driver);
    }
}

fn payload() -> Vec<u8> {
    (0..12_000u32).map(|i| (i % 251) as u8).collect()
}

struct ProbeApp {
    payload: Option<Vec<u8>>,
    closes: Rc<Cell<u32>>,
}

impl Application for ProbeApp {
    type Conn = ();
    type Wire = GracefulWire;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> Outcome {
        let Some(reply) = self.payload.as_ref() else {
            return Outcome::Ok;
        };
        let n = reply.len();
        let buf = aux.write_buf_for(slot);
        buf[..n].copy_from_slice(reply);
        let ud = slot.token();
        slot.submit_buffered(buf, n, ud, driver);
        Outcome::CloseAfter
    }

    fn on_send(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) {
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
        self.closes.set(self.closes.get() + 1);
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App {
    #[pin]
    #[manifold]
    listener: Listener<0, ProbeApp, Bundle<Tcp, GracefulWire, profile::Throughput>>,
}

fn build(
    drv: &mut Driver,
    payload: Option<Vec<u8>>,
    closes: Rc<Cell<u32>>,
) -> (
    Listener<0, ProbeApp, Bundle<Tcp, GracefulWire, profile::Throughput>>,
    SocketAddr,
) {
    let bind: SocketAddr = "127.0.0.1:0".parse().expect("parse");
    let cfg = Config::<Tcp> {
        max_conn: 64,
        bind,
        backlog: 128,
        stream_opts: Default::default(),
        listener_opts: ListenerOpts::default(),
    };
    let listener = Listener::<0, ProbeApp, Bundle<Tcp, GracefulWire, profile::Throughput>>::open_in(
        ProbeApp { payload, closes },
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

fn make_exec() -> Executor {
    let cfg = <dope::DriverCfg as DriverConfig>::for_tcp_profile::<profile::Throughput>(64);
    Executor::new(cfg).expect("executor")
}

#[test]
fn graceful_sentinel_trails_drain_reply() {
    let want = payload();
    let closes = Rc::new(Cell::new(0u32));
    let mut exec = make_exec();
    let (listener, addr) = build(exec.driver_mut(), Some(want.clone()), closes.clone());
    let mut app = pin!(App { listener });

    let want_len = want.len();
    let handle = std::thread::spawn(move || {
        let mut s = wait_for_addr(addr);
        s.set_read_timeout(Some(Duration::from_secs(3))).ok();
        s.write_all(b"GET\n").expect("write request");
        let mut got = Vec::new();
        s.read_to_end(&mut got).ok();
        got
    });

    let closes_done = closes.clone();
    drive_until(&mut exec, app.as_mut(), move || closes_done.get() >= 1);

    let got = handle.join().expect("client join");
    assert_eq!(got.len(), want_len + BYE.len(), "reply + graceful sentinel length");
    assert_eq!(&got[..want_len], &want[..], "reply bytes precede sentinel");
    assert_eq!(&got[want_len..], BYE, "graceful sentinel trails the reply before FIN");
    assert_eq!(closes.get(), 1, "connection must close exactly once");
}

#[test]
fn graceful_sentinel_survives_peer_eof() {
    let closes = Rc::new(Cell::new(0u32));
    let mut exec = make_exec();
    let (listener, addr) = build(exec.driver_mut(), None, closes.clone());
    let mut app = pin!(App { listener });

    let handle = std::thread::spawn(move || {
        let mut s = wait_for_addr(addr);
        s.set_read_timeout(Some(Duration::from_secs(3))).ok();
        s.write_all(b"REQ").expect("write request");
        s.shutdown(Shutdown::Write).expect("half close");
        let mut got = Vec::new();
        s.read_to_end(&mut got).ok();
        got
    });

    let closes_done = closes.clone();
    drive_until(&mut exec, app.as_mut(), move || closes_done.get() >= 1);

    let got = handle.join().expect("client join");
    assert_eq!(got, BYE, "peer EOF must still emit the graceful sentinel, not suppress it");
    assert_eq!(closes.get(), 1, "connection must close exactly once");
}
