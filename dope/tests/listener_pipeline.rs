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

const A_LEN: usize = 8000;
const B_LEN: usize = 9000;

fn resp_a() -> Vec<u8> {
    vec![0xA1; A_LEN]
}
fn resp_b() -> Vec<u8> {
    vec![0xB2; B_LEN]
}

// Commits two responses in one on_chunk: the first goes in-flight (fast path,
// primary write_buf), the second is requested WHILE the first is in flight.
// With the structural guard, the second write_buf_for hands back a scratch
// buffer (not the in-flight one), and submit_buffered queues it — so both
// responses arrive intact and in order, no double-submit corruption.
struct PipelineApp {
    closes: Rc<Cell<u32>>,
}

impl Application for PipelineApp {
    type Conn = ();
    type Wire = Identity;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> Outcome {
        let ud = slot.token();
        let a = resp_a();
        let buf = aux.write_buf_for(slot);
        buf[..a.len()].copy_from_slice(&a);
        slot.submit_buffered(buf, a.len(), ud, driver);

        let b = resp_b();
        let buf2 = aux.write_buf_for(slot);
        buf2[..b.len()].copy_from_slice(&b);
        slot.submit_buffered(buf2, b.len(), ud, driver);

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
    listener: Listener<0, PipelineApp, Bundle<Tcp, Identity, profile::Throughput>>,
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
    Listener<0, PipelineApp, Bundle<Tcp, Identity, profile::Throughput>>,
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
    let listener = Listener::<0, PipelineApp, Bundle<Tcp, Identity, profile::Throughput>>::open_in(
        PipelineApp { closes },
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
fn two_responses_committed_while_first_in_flight_arrive_in_order() {
    let mut want = resp_a();
    want.extend_from_slice(&resp_b());
    let closes = Rc::new(Cell::new(0u32));
    let mut exec = make_exec(64);
    let drv = exec.driver_mut();
    let (listener, addr) = build_listener(drv, 64, closes.clone());
    let mut app = pin!(App { listener });

    let want_len = want.len();
    let handle = std::thread::spawn(move || {
        let mut s = wait_for_addr(addr);
        s.set_read_timeout(Some(Duration::from_secs(3))).ok();
        s.write_all(b"GO\n").expect("write request");
        let mut got = vec![0u8; want_len];
        let ok = s.read_exact(&mut got).is_ok();
        (ok, got)
    });

    let closes_done = closes.clone();
    drive_until(&mut exec, app.as_mut(), move || closes_done.get() >= 1);

    let (ok, got) = handle.join().expect("client join");
    assert!(ok, "did not receive {} bytes (corruption/truncation)", want.len());
    assert_eq!(got, want, "responses corrupted or reordered on the pipelined path");
    assert_eq!(closes.get(), 1, "connection must close exactly once");
}
