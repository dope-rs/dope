use std::{net, pin, time};

use dope_manifold::datagram::{Endpoint, Handler, OwnedSuffix, Socket};
use dope_test::fibers::Gate;

struct Sender {
    destination: net::SocketAddr,
    pending: Option<OwnedSuffix>,
    queued: Gate,
}

impl<'d> Handler<'d, 0> for Sender {
    fn packet<'turn>(
        &mut self,
        _addr: net::SocketAddr,
        _packet: dope_manifold::datagram::packet::Packet<'turn, 'd>,
        _socket: pin::Pin<&'turn mut Socket<'d, 0>>,
        _now: time::Instant,
    ) {
    }

    fn pre_park<'turn>(
        &mut self,
        mut socket: pin::Pin<&mut Socket<'d, 0>>,
        _now: time::Instant,
        _work: dope_core::driver::schedule::Application<'turn, 'd>,
    ) {
        let Some(payload) = self.pending.take() else {
            return;
        };
        match socket.as_mut().queue_suffix(payload, self.destination) {
            Ok(()) => self.queued.hit(),
            Err(payload) => self.pending = Some(payload),
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
    endpoint: Endpoint<'d, 0, Sender>,
}

#[test]
fn owned_suffix_sends_only_its_validated_view() {
    let receiver = net::UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    receiver
        .set_read_timeout(Some(dope_test::GUARD / 2))
        .expect("set timeout");
    let destination = receiver.local_addr().expect("receiver address");
    let queued = Gate::new();
    let storage = b"pseudo-prefixretry-wire".to_vec();
    let payload = OwnedSuffix::new(storage, b"pseudo-prefix".len()).expect("valid suffix");

    dope_test::scenario::rt::Runtime::quic(4096, 2048)
        .executor()
        .enter(|mut session| {
            let endpoint = Endpoint::bind(
                "127.0.0.1:0".parse().expect("sender address"),
                Sender {
                    destination,
                    pending: Some(payload),
                    queued: queued.clone(),
                },
                &mut session.driver_access(),
            )
            .expect("bind sender");

            session
                .with_app(App { endpoint }, |mut app| {
                    dope_test::fibers::TEST.run_until(&mut app, &queued, 1);
                    let mut received = [0; 64];
                    let (len, _) = receiver.recv_from(&mut received).expect("receive suffix");
                    assert_eq!(&received[..len], b"retry-wire");
                })
                .expect("application teardown");
        });
}
