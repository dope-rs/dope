use std::{net::SocketAddr, pin::Pin};

use dope_core::driver::settings;
use dope_manifold::datagram::{Endpoint, Handler, Socket, packet::Packet};
use dope_runtime::executor::Executor;

struct IgnorePackets;

impl<'d> Handler<'d, 0> for IgnorePackets {
    fn packet<'turn>(
        &mut self,
        _addr: SocketAddr,
        _packet: Packet<'turn, 'd>,
        _socket: Pin<&'turn mut Socket<'d, 0>>,
        _now: std::time::Instant,
    ) {
    }
}

fn reserve_udp_addr() -> SocketAddr {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve port");
    sock.local_addr().expect("local addr")
}

#[test]
fn datagram_fixed_port_allows_concurrent_reuseport_binds() {
    for _ in 0..5 {
        let addr = reserve_udp_addr();
        assert_ne!(addr.port(), 0);

        let exec_a = dope_test::scenario::rt::Runtime::quic(4096, 2048).executor();
        let bound = exec_a.enter(|mut sess_a| {
            let Ok(endpoint_a) =
                Endpoint::<0, _>::bind(addr, IgnorePackets, &mut sess_a.driver_access())
            else {
                return false;
            };

            sess_a
                .with_app(dope_test::scenario::ManifoldHost::new(endpoint_a), |_| {
                    let exec_b = dope_test::scenario::rt::Runtime::quic(4096, 2048).executor();
                    exec_b.enter(|mut sess_b| {
                        let endpoint_b = Endpoint::<0, _>::bind(
                            addr,
                            IgnorePackets,
                            &mut sess_b.driver_access(),
                        )
                        .expect("second bind on same fixed port must succeed with SO_REUSEPORT");

                        assert_eq!(endpoint_b.local_addr().port(), addr.port());
                        sess_b
                            .with_app(dope_test::scenario::ManifoldHost::new(endpoint_b), |_| {})
                            .expect("second socket teardown");
                    });
                })
                .expect("first socket teardown");
            true
        });
        if bound {
            return;
        }
    }
    panic!("reserved port kept racing away across retries");
}

#[test]
fn finished_datagram_reuses_its_only_fixed_slot() {
    let config = settings::Config::for_quic_udp(64, 2048)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<0, 1>());
    Executor::new(config)
        .expect("executor")
        .enter(|mut session| {
            for _ in 0..8 {
                let endpoint = Endpoint::<0, _>::bind(
                    "127.0.0.1:0".parse().expect("address"),
                    IgnorePackets,
                    &mut session.driver_access(),
                )
                .expect("fixed slot must be reclaimed");
                session
                    .with_app(dope_test::scenario::ManifoldHost::new(endpoint), |_| {})
                    .expect("application teardown");
            }
        });
}
