#![cfg(target_os = "linux")]
//! `queue_segmented` hands the kernel UDP_SEGMENT sends that arrive as N separate
//! datagrams (the last possibly shorter), chunking runs past the 64-segment /
//! 65535-byte cap across multiple sends. Where GSO is missing the kernel rejects
//! it with EINVAL; the test skips rather than failing.

mod common;

use std::cell::Cell;
use std::net::{SocketAddr, UdpSocket};
use std::pin::{Pin, pin};
use std::rc::Rc;

use dope::manifold::Manifold;
use dope::manifold::datagram::{Handler, Socket};
use dope::runtime::dispatcher::Idle;
use dope::{Driver, DriverCfg, DriverConfig, Event, Executor};

use common::Gate;

fn payload(len: usize) -> Vec<u8> {
    (0..len as u32).map(|i| (i % 251) as u8).collect()
}

struct SendHandler {
    errno: Rc<Cell<Option<i32>>>,
    gate: Rc<Gate>,
}

impl Handler<0> for SendHandler {
    fn on_packet(&mut self, _addr: SocketAddr, _data: &[u8], _sock: Pin<&mut Socket<0>>) {}

    fn on_error(&mut self, errno: i32, _sock: Pin<&mut Socket<0>>) {
        self.errno.set(Some(errno));
        self.gate.hit();
    }
}

#[pin_project::pin_project]
struct Sender<'d> {
    #[pin]
    sock: Socket<'d, 0>,
    handler: SendHandler,
}

impl<'d> Manifold<'d> for Sender<'d> {
    const ID: u8 = 0;

    fn dispatch(self: Pin<&mut Self>, ev: Event, driver: &'d Driver) {
        let mut this = self.project();
        match ev {
            Event::Recv(token, more, e) => {
                this.sock
                    .dispatch_recv(token, more, e, driver, this.handler)
            }
            Event::Send(token, e) => {
                this.sock.as_mut().dispatch_send(token, e, this.handler);
                if !this.sock.has_pending() {
                    this.handler.gate.hit();
                }
            }
            _ => {}
        }
    }

    fn pre_park(self: Pin<&mut Self>, driver: &'d Driver) {
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
struct App<'d> {
    #[pin]
    #[manifold]
    sender: Sender<'d>,
    #[pin]
    #[manifold]
    guard: common::Guard,
}

/// A run of segments must arrive as exactly that many datagrams, byte-identical,
/// however many sendmsg calls dope splits it into.
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
    let want_datagrams = lens.len();
    let recv = UdpSocket::bind("127.0.0.1:0").expect("bind recv");
    recv.set_read_timeout(Some(common::GUARD / 2)).ok();
    let dst = recv.local_addr().expect("recv addr");

    let collector = std::thread::spawn(move || {
        let mut got = Vec::new();
        let mut seen = 0usize;
        let mut buf = [0u8; 2048];
        for _ in 0..want_datagrams {
            match recv.recv_from(&mut buf) {
                Ok((n, _)) => {
                    got.extend_from_slice(&buf[..n]);
                    seen += 1;
                }
                Err(_) => break,
            }
        }
        (seen, got)
    });

    let gate = Gate::new();
    let errno = Rc::new(Cell::new(None));
    let cfg = <DriverCfg as DriverConfig>::for_quic_udp(4096, 2048);
    let exec = Executor::new(cfg).expect("executor");
    let mut sess = exec.enter();
    let sock =
        Socket::<0>::bind("127.0.0.1:0".parse().expect("parse"), sess.driver()).expect("bind send");
    let mut app = pin!(App {
        sender: Sender {
            sock,
            handler: SendHandler {
                errno: errno.clone(),
                gate: gate.clone(),
            },
        },
        guard: common::Guard::new(),
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

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 1);
    let (seen, got) = collector.join().expect("collector join");

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
    assert_eq!(seen, want_datagrams, "expected {want_datagrams} datagrams");
    assert_eq!(got, want, "reassembled GSO payload differs from source");
}
