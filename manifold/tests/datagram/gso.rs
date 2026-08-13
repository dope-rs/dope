use std::{
    net::{SocketAddr, UdpSocket},
    num::NonZeroU16,
    pin::Pin,
    thread::JoinHandle,
    time::Instant,
};

use dope_manifold::datagram::{self, Endpoint, Handler, Socket};
use dope_test::fibers::Gate;

struct SendHandler {
    queued: Gate,
    pending: Option<(Vec<u8>, NonZeroU16, SocketAddr)>,
}

impl<'d> Handler<'d, 0> for SendHandler {
    fn packet<'turn>(
        &mut self,
        _addr: SocketAddr,
        _packet: dope_manifold::datagram::packet::Packet<'turn, 'd>,
        _sock: Pin<&'turn mut Socket<'d, 0>>,
        _now: Instant,
    ) {
    }

    fn pre_park<'turn>(
        &mut self,
        mut socket: Pin<&mut Socket<'d, 0>>,
        _now: Instant,
        _work: dope_core::driver::schedule::Application<'turn, 'd>,
    ) {
        let Some((payload, segment_size, destination)) = self.pending.take() else {
            return;
        };
        match socket
            .as_mut()
            .queue_gso(payload, segment_size, destination)
        {
            Ok(()) => self.queued.hit(),
            Err(payload) => self.pending = Some((payload, segment_size, destination)),
        }
    }

    fn progress(
        &self,
        _region: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        if self.pending.is_some() {
            dope_core::driver::schedule::Progress::Runnable
        } else {
            dope_core::driver::schedule::Progress::Quiescent
        }
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(
    core = dope_core,
    manifold = dope_manifold,
    runtime = dope_runtime,
    region = o3::cell::region::Token,
)]
struct App<'d> {
    #[pin]
    #[manifold]
    sender: Endpoint<'d, 0, SendHandler>,
}

#[test]
fn gso_runs_as_one_owned_send() {
    if datagram::GSO_LIMITS.is_none() {
        return;
    }
    run_case(&[1000, 1000, 1000, 500]);
    let mut lens = vec![1000usize; 63];
    lens.push(500);
    run_case(&lens);
}

fn udp_collector(want_datagrams: usize) -> (SocketAddr, JoinHandle<(usize, Vec<u8>)>) {
    let recv = UdpSocket::bind("127.0.0.1:0").expect("bind recv");
    recv.set_read_timeout(Some(dope_test::GUARD / 2)).ok();
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

fn run_case(lens: &[usize]) {
    let total: usize = lens.iter().sum();
    let want_datagrams = lens.len();
    let (dst, collector) = udp_collector(want_datagrams);

    let queued = Gate::new();
    dope_test::scenario::rt::Runtime::quic(4096, 2048)
        .executor()
        .enter(|mut sess| {
            let want = dope_test::peer::Pattern::with_len(total).into_bytes();
            let segment_size = NonZeroU16::new(lens[0] as u16).expect("non-zero segment size");
            let sender = Endpoint::bind(
                "127.0.0.1:0".parse().expect("parse"),
                SendHandler {
                    queued: queued.clone(),
                    pending: Some((want.clone(), segment_size, dst)),
                },
                &mut sess.driver_access(),
            )
            .expect("bind send");
            let app = App { sender };

            sess.with_app(app, |mut app| {
                dope_test::fibers::TEST.run_until(&mut app, &queued, 1);
                let (seen, got) = collector.join().expect("collector join");

                assert_eq!(seen, want_datagrams, "expected {want_datagrams} datagrams");
                assert_eq!(got, want, "reassembled GSO payload differs from source");
            })
            .expect("application teardown");
        });
}
