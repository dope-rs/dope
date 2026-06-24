#![cfg(target_os = "linux")]

use std::cell::Cell;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
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

fn payload() -> Vec<u8> {
    (0..12_000u32).map(|i| (i % 251) as u8).collect()
}

struct ReplyApp {
    payload: Vec<u8>,
    closes: Rc<Cell<u32>>,
}

impl Application for ReplyApp {
    type Conn = ();
    type Wire = Identity;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> Outcome {
        let n = self.payload.len();
        let buf = aux.write_buf_for(slot);
        buf[..n].copy_from_slice(&self.payload);
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
    listener: Listener<0, ReplyApp, Bundle<Tcp, Identity, profile::Throughput>>,
}

fn make_exec(max_conn: usize) -> Executor {
    let cfg = <dope::DriverCfg as DriverConfig>::for_tcp_profile::<profile::Throughput>(max_conn);
    Executor::new(cfg).expect("executor")
}

fn build_listener(
    drv: &mut Driver,
    max_conn: usize,
    closes: Rc<Cell<u32>>,
) -> (
    Listener<0, ReplyApp, Bundle<Tcp, Identity, profile::Throughput>>,
    SocketAddr,
) {
    let bind: SocketAddr = "127.0.0.1:0".parse().expect("parse");
    let cfg = Config::<Tcp> {
        max_conn,
        bind,
        backlog: 128,
        stream_opts: Default::default(),
        listener_opts: ListenerOpts::default(),
    };
    let listener = Listener::<0, ReplyApp, Bundle<Tcp, Identity, profile::Throughput>>::open_in(
        ReplyApp {
            payload: payload(),
            closes,
        },
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

// A buffered reply staged on the close-after (Draining) path must be delivered
// in full before the FIN. This is the F1 regression guard: an empty/truncated
// close would surface here as a short or empty client read.
#[test]
fn drain_reply_is_delivered_in_full_before_close() {
    let want = payload();
    let closes = Rc::new(Cell::new(0u32));
    let mut exec = make_exec(64);
    let drv = exec.driver_mut();
    let (listener, addr) = build_listener(drv, 64, closes.clone());
    let mut app = pin!(App { listener });

    let want_len = want.len();
    let handle = std::thread::spawn(move || {
        let mut s = wait_for_addr(addr);
        s.set_read_timeout(Some(Duration::from_secs(3))).ok();
        s.write_all(b"GET\n").expect("write request");
        let mut got = vec![0u8; want_len];
        let ok = s.read_exact(&mut got).is_ok();
        (ok, got)
    });

    let closes_done = closes.clone();
    drive_until(&mut exec, app.as_mut(), move || closes_done.get() >= 1);

    let (ok, got) = handle.join().expect("client join");
    assert!(ok, "reply truncated before {} bytes (empty/short close)", want.len());
    assert_eq!(got, want, "reply bytes corrupted on the drain path");
    assert_eq!(closes.get(), 1, "connection must close exactly once");
}
