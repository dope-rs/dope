//! Fixed-port datagram (QUIC server) sockets must enable SO_REUSEPORT so every
//! per-core worker can bind the same port. Before the fix the second bind failed
//! with EADDRINUSE, pinning QUIC to a single core.

use dope::manifold::datagram::Socket;
use dope::{DriverCfg, DriverConfig, Executor};

#[test]
fn datagram_fixed_port_allows_concurrent_reuseport_binds() {
    let addr: std::net::SocketAddr = "127.0.0.1:54983".parse().unwrap();

    let cfg_a = <DriverCfg as DriverConfig>::for_quic_udp(4096, 2048);
    let exec_a = Executor::new(cfg_a).expect("executor a");
    let sess_a = exec_a.enter();
    let _sock_a = Socket::<0>::bind(addr, sess_a.driver()).expect("first bind");

    let cfg_b = <DriverCfg as DriverConfig>::for_quic_udp(4096, 2048);
    let exec_b = Executor::new(cfg_b).expect("executor b");
    let sess_b = exec_b.enter();
    let sock_b = Socket::<0>::bind(addr, sess_b.driver())
        .expect("second bind on same fixed port must succeed with SO_REUSEPORT");

    assert_eq!(sock_b.local_addr().port(), 54983);
}
