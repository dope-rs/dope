#![cfg(target_os = "linux")]

//! `queue_segmented` hands the kernel UDP_SEGMENT sends that arrive as N
//! separate datagrams (the last possibly shorter), chunking runs past the
//! 64-segment / 65535-byte cap across multiple sends. Where GSO is missing the
//! kernel rejects it with EINVAL; the test skips rather than failing.

use std::cell::Cell;
use std::net::{SocketAddr, UdpSocket};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dope::fiber::Fiber;
use dope::manifold::Manifold;
use dope::manifold::datagram::{Handler, Socket};
use dope::runtime::dispatcher::Idle;
use dope::{Driver, DriverCfg, DriverConfig, Event, Executor};

fn payload(len: usize) -> Vec<u8> {
    (0..len as u32).map(|i| (i % 251) as u8).collect()
}

struct SendHandler {
    errno: Rc<Cell<Option<i32>>>,
}

impl Handler<0> for SendHandler {
    fn on_packet(&mut self, _addr: SocketAddr, _data: &[u8], _sock: Pin<&mut Socket<0>>) {}

    fn on_error(&mut self, errno: i32, _sock: Pin<&mut Socket<0>>) {
        self.errno.set(Some(errno));
    }
}

#[pin_project::pin_project]
struct Sender {
    #[pin]
    sock: Socket<0>,
    handler: SendHandler,
}

impl Manifold for Sender {
    const ID: u8 = 0;

    fn dispatch(self: Pin<&mut Self>, ev: Event, driver: &mut Driver) {
        let this = self.project();
        match ev {
            Event::Recv(token, more, e) => {
                this.sock
                    .dispatch_recv(token, more, e, driver, this.handler)
            }
            Event::Send(token, e) => this.sock.dispatch_send(token, e, this.handler),
            _ => {}
        }
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut Driver) {
        self.project().sock.tick(driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        if self.project_ref().sock.has_pending() {
            Idle::Busy
        } else {
            Idle::Park(None)
        }
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App {
    #[pin]
    #[manifold]
    sender: Sender,
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

/// A run of `segments` packets of `seg` bytes (the last `tail` shorter) must
/// arrive as exactly that many datagrams, byte-identical, however many sendmsg
/// calls dope splits it into.
#[test]
fn gso_run_within_cap_is_one_send() {
    run_case(1000, &[1000, 1000, 1000, 500]);
}

/// 100 segments exceeds the 64-segment cap, so dope must split into multiple
/// UDP_SEGMENT sends — the receiver still sees 100 datagrams.
#[test]
fn gso_run_past_cap_splits_across_sends() {
    let mut lens = vec![1200usize; 99];
    lens.push(500);
    run_case(1200, &lens);
}

fn run_case(seg: u16, lens: &[usize]) {
    let total: usize = lens.iter().sum();
    let expect_datagrams = lens.len();
    let recv = UdpSocket::bind("127.0.0.1:0").expect("bind recv");
    recv.set_read_timeout(Some(Duration::from_secs(2))).ok();
    let dst = recv.local_addr().expect("recv addr");

    let received = Arc::new(AtomicUsize::new(0));
    let received_thread = received.clone();
    let collector = std::thread::spawn(move || {
        let mut got = Vec::new();
        let mut buf = [0u8; 2048];
        for _ in 0..expect_datagrams {
            match recv.recv_from(&mut buf) {
                Ok((n, _)) => {
                    got.extend_from_slice(&buf[..n]);
                    received_thread.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => break,
            }
        }
        got
    });

    let cfg = <DriverCfg as DriverConfig>::for_quic_udp(4096, 2048);
    let mut exec = Executor::new(cfg).expect("executor");
    let errno = Rc::new(Cell::new(None));
    let sock = Socket::<0>::bind("127.0.0.1:0".parse().unwrap(), exec.driver_mut()).expect("bind");
    let mut app = pin!(App {
        sender: Sender {
            sock,
            handler: SendHandler {
                errno: errno.clone(),
            },
        },
    });

    let want = payload(total);
    let queued = app
        .as_mut()
        .project()
        .sender
        .project()
        .sock
        .queue_segmented(want.clone(), dst, seg);
    assert!(queued, "queue_segmented must accept the send");

    let recv_done = received.clone();
    let err_seen = errno.clone();
    drive_until(&mut exec, app.as_mut(), move || {
        recv_done.load(Ordering::Relaxed) >= expect_datagrams || err_seen.get().is_some()
    });

    let got = collector.join().expect("collector join");

    if errno.get() == Some(libc::EINVAL) {
        eprintln!("UDP GSO unsupported on this kernel (EINVAL); skipping");
        return;
    }
    assert_eq!(
        errno.get(),
        None,
        "send failed with errno {:?}",
        errno.get()
    );

    assert_eq!(
        received.load(Ordering::Relaxed),
        expect_datagrams,
        "expected {expect_datagrams} segmented datagrams"
    );
    assert_eq!(got, want, "reassembled GSO payload differs from source");
}
