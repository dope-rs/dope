#![cfg(target_os = "linux")]

use dope_test as common;

extern crate dope;
use o3::cell::BrandCell;

use std::cell::Cell;
use std::net::{SocketAddr, UdpSocket};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::thread::JoinHandle;

use dope::manifold::Manifold;
use dope::manifold::datagram::{Handler, Socket};
use dope::runtime::dispatcher::Idle;
use dope::{Event, EventKind};

use common::Gate;

struct SendHandler {
    errno: Rc<Cell<Option<i32>>>,
    gate: Rc<Gate>,
}

impl<'d> Handler<'d, 0> for SendHandler {
    fn packet(
        &mut self,
        _addr: SocketAddr,
        packet: dope::manifold::datagram::Packet<'d>,
        _sock: Pin<&mut Socket<'d, 0>>,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        packet.release(driver);
    }

    fn error(&mut self, errno: i32, _sock: Pin<&mut Socket<0>>) {
        self.errno.set(Some(errno));
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

    fn dispatch(mut self: Pin<&mut Self>, ev: Event<'d>, driver: &mut dope::DriverContext<'_, 'd>) {
        let mut sender = self.as_mut();
        let mut this = sender.as_mut().project();
        match ev.into_kind() {
            EventKind::Recv(token, more, e) => {
                this.sock
                    .dispatch_recv(token, more, e, this.handler, driver)
            }
            EventKind::Send(token, e) => {
                this.sock
                    .as_mut()
                    .dispatch_send(token, e, this.handler, driver);
                this.handler.gate.hit();
            }
            _ => {}
        }
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut dope::DriverContext<'_, 'd>) {
        self.as_mut().project().sock.tick(driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        if self.project_ref().sock.needs_flush() {
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
}

#[test]
fn gso_runs_within_and_past_cap() {
    run_case(&[1000, 1000, 1000, 500], 1);
    let mut lens = vec![1200usize; 99];
    lens.push(500);
    run_case(&lens, 2);
}

#[test]
fn dropping_armed_socket_poison_route() {
    common::quic_exec(64, 2048).enter(|mut sess| {
        {
            let mut driver = sess.driver_access();
            let socket = Socket::<0>::bind("127.0.0.1:0".parse().expect("parse"), &mut driver)
                .expect("bind");
            let mut socket = pin!(socket);
            socket.as_mut().tick(&mut driver);
        }
        let error = match Socket::<0>::bind(
            "127.0.0.1:0".parse().expect("parse"),
            &mut sess.driver_access(),
        ) {
            Ok(_) => panic!("dirty route reused"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    });
}

fn udp_collector(want_datagrams: usize) -> (SocketAddr, JoinHandle<(usize, Vec<u8>)>) {
    let recv = UdpSocket::bind("127.0.0.1:0").expect("bind recv");
    recv.set_read_timeout(Some(common::GUARD / 2)).ok();
    let dst = recv.local_addr().expect("recv addr");
    let handle = std::thread::spawn(move || {
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
    (dst, handle)
}

fn run_case(lens: &[usize], sends: u32) {
    let total: usize = lens.iter().sum();
    let want_datagrams = lens.len();
    let (dst, collector) = udp_collector(want_datagrams);

    let gate = Gate::new();
    let errno = Rc::new(Cell::new(None));
    common::quic_exec(4096, 2048).enter(|mut sess| {
        let sock = Socket::<0>::bind(
            "127.0.0.1:0".parse().expect("parse"),
            &mut sess.driver_access(),
        )
        .expect("bind send");
        let app = pin!(BrandCell::new(App {
            sender: Sender {
                sock,
                handler: SendHandler {
                    errno: errno.clone(),
                    gate: gate.clone(),
                },
            },
        }));

        let want = common::pattern(total);
        let segments: Vec<_> = lens.iter().map(|&len| len as u32).collect();
        let queued = app
            .as_ref()
            .borrow_pin_mut(sess.token())
            .project()
            .sender
            .project()
            .sock
            .queue_segments(want.clone(), &segments, dst);
        assert!(queued.is_ok(), "queue_segments must accept the send");

        {
            let (token, mut driver) = sess.token_and_driver();
            let mut app = app.as_ref().borrow_pin_mut(token);
            let mut sender = app.as_mut().project().sender.project();
            sender.sock.as_mut().tick(&mut driver);
        }

        common::run_until(&mut sess, app.as_ref(), &gate, sends);
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
    });
}
