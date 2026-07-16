extern crate dope;

mod common;

use std::net::SocketAddr;

use dope::manifold::datagram::Socket;

fn reserve_udp_addr() -> SocketAddr {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve port");
    sock.local_addr().expect("local addr")
}

#[test]
fn datagram_fixed_port_allows_concurrent_reuseport_binds() {
    for _ in 0..5 {
        let addr = reserve_udp_addr();
        assert_ne!(addr.port(), 0);

        let exec_a = common::quic_exec(4096, 2048);
        let bound = exec_a.enter(|mut sess_a| {
            let Ok(_sock_a) = Socket::<0>::bind(addr, &mut sess_a.driver_access()) else {
                return false;
            };

            let exec_b = common::quic_exec(4096, 2048);
            exec_b.enter(|mut sess_b| {
                let sock_b = Socket::<0>::bind(addr, &mut sess_b.driver_access())
                    .expect("second bind on same fixed port must succeed with SO_REUSEPORT");

                assert_eq!(sock_b.local_addr().port(), addr.port());
            });
            true
        });
        if bound {
            return;
        }
    }
    panic!("reserved port kept racing away across retries");
}
