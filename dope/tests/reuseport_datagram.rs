extern crate dope;

use std::net::SocketAddr;
use std::pin::Pin;

use dope::DriverContext;
use dope::manifold::Manifold;
use dope::manifold::datagram::Socket;
use dope::runtime::dispatcher::FinishContext;
use dope::runtime::executor::Executor;

#[pin_project::pin_project]
struct SocketHost<'d> {
    #[pin]
    socket: Socket<'d, 0>,
}

impl<'d> Manifold<'d> for SocketHost<'d> {
    fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().project().socket.tick(driver);
    }

    fn shutdown(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().project().socket.shutdown(driver);
    }

    fn finish(mut self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        self.as_mut().project().socket.finish(context);
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

        let exec_a = dope_test::quic_exec(4096, 2048);
        let bound = exec_a.enter(|mut sess_a| {
            let Ok(_sock_a) = Socket::<0>::bind(addr, &mut sess_a.driver_access()) else {
                return false;
            };

            let exec_b = dope_test::quic_exec(4096, 2048);
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

#[test]
fn finished_datagram_reuses_its_only_fixed_slot() {
    let mut config = dope::driver::Config::for_quic_udp(64, 2048);
    config.fixed_file_slots = 1;
    Executor::new(config)
        .expect("executor")
        .enter(|mut session| {
            for _ in 0..8 {
                let socket = Socket::bind(
                    "127.0.0.1:0".parse().expect("address"),
                    &mut session.driver_access(),
                )
                .expect("fixed slot must be reclaimed");
                session.with_app(dope_test::ManifoldHost::new(SocketHost { socket }), |_| {});
            }
        });
}
