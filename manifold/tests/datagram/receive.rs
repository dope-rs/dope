use std::{
    net::{SocketAddr, UdpSocket},
    pin::Pin,
};

use dope_manifold::datagram::{Endpoint, Handler, Socket, packet::Packet};
use dope_test::fibers::Gate;

struct Expected {
    gate: Gate,
    payload: &'static [u8],
}

impl<'d> Handler<'d, 0> for Expected {
    fn packet<'turn>(
        &mut self,
        _source: SocketAddr,
        packet: Packet<'turn, 'd>,
        _socket: Pin<&'turn mut Socket<'d, 0>>,
        _now: std::time::Instant,
    ) {
        assert_eq!(packet.as_ref(), self.payload);
        self.gate.hit();
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
    receiver: Endpoint<'d, 0, Expected>,
}

fn assert_packet(payload: &'static [u8]) {
    let gate = Gate::new();
    let capacity = u32::try_from(payload.len().max(1)).expect("test payload capacity");
    dope_test::scenario::rt::Runtime::quic(2, capacity)
        .executor()
        .enter(|mut session| {
            let receiver = Endpoint::bind(
                "127.0.0.1:0".parse().expect("receive address"),
                Expected {
                    gate: gate.clone(),
                    payload,
                },
                &mut session.driver_access(),
            )
            .expect("bind receiver");
            let destination = receiver.local_addr();
            let sender = UdpSocket::bind("127.0.0.1:0").expect("bind sender");
            assert_eq!(
                sender.send_to(payload, destination).expect("send datagram"),
                payload.len()
            );

            session
                .with_app(App { receiver }, |mut app| {
                    dope_test::fibers::TEST.run_until(&mut app, &gate, 1);
                })
                .expect("application teardown");
        });
}

#[test]
fn zero_length_datagram_is_a_packet() {
    assert_packet(&[]);
}

#[test]
fn one_byte_datagram_fits_the_minimum_slot() {
    assert_packet(&[0xA5]);
}
