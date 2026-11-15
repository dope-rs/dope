pub mod common;

use std::{convert::Infallible, time::Instant};

use dope::{
    core::driver::{
        life::{creation::Attempt, recovery},
        route::Id,
        settings::{self, claim::Needs},
    },
    manifold::app::Host,
    runtime::executor::{Executor, session},
};
use dope_quic::{
    conn::life::options::Options,
    runtime::endpoint::{config::Config, server},
};
use dope_test::{
    fibers,
    harness::{peer::udp::Socket, scenario::TIMEOUT},
};

use common::runtime::udp::{Capture, REQUESTS};

#[test]
fn native_datagram_custody() {
    for size in [90, 2401] {
        let sender = Socket::bind(([127, 0, 0, 1], 0).into()).expect("peer socket");
        sender.read_wait(Some(TIMEOUT)).expect("bounded receive");
        let peer = sender.local_addr().expect("peer address");
        let plan = server::Plan::<0>::new(
            ([127, 0, 0, 1], 0).into(),
            common::peer::raw(0x36)
                .expect("identity")
                .0
                .server()
                .expect("server TLS"),
            Options::default(),
            common::routing::CONFIG,
        )
        .expect("retained server plan");
        let settings = Needs::from_tree(plan.as_ref())
            .expect("endpoint claims")
            .apply_exact(Config::driver(settings::Datagram::fixed::<2, 1200>()))
            .expect("driver configuration");
        let mut creation = Attempt::new();
        let executor = Executor::new(&mut creation, settings)
            .map_err(|error| format!("{:?}", error.kind()))
            .expect("driver creation");
        let mut reports = Vec::new();
        let address = executor.run(|executor| {
            *executor
                .enter_with(
                    |mut session| {
                        let endpoint = plan
                            .bind(
                                Capture {
                                    peer,
                                    size,
                                    classified: 0,
                                    settled: 0,
                                    ingress: 0,
                                    rejected: false,
                                    pending: None,
                                    pointer: std::ptr::null(),
                                    packet_errors: 0,
                                    failure: None,
                                },
                                &mut session.driver(),
                            )
                            .expect("native endpoint");
                        let address = endpoint.local_addr();
                        let requests = if size == 90 {
                            &REQUESTS[..]
                        } else {
                            &REQUESTS[..1]
                        };
                        for packet in requests {
                            assert_eq!(
                                sender.send_to(packet, address).expect("request"),
                                packet.len()
                            );
                        }
                        let mut finish = session::Completion::new();
                        session
                            .with_quiesce(&mut finish, Host::new(endpoint), TIMEOUT, |mut app| {
                                let deadline = Instant::now() + TIMEOUT;
                                loop {
                                    let complete = app.inspect(Host::manifold, |endpoint| {
                                        let events = endpoint.handler();
                                        assert!(events.failure.is_none(), "{:?}", events.failure);
                                        if size == 90 {
                                            events.ingress == 1
                                        } else {
                                            events.rejected
                                        }
                                    });
                                    if complete {
                                        break;
                                    }
                                    assert!(Instant::now() < deadline, "native raw progress");
                                    fibers::TEST
                                        .try_run(&mut app, fibers::Yield::new())
                                        .expect("native receive and completion");
                                }
                                app.inspect(Host::manifold, |endpoint| {
                                    let events = endpoint.handler();
                                    assert!(events.pending.is_none());
                                    if size == 90 {
                                        assert_eq!(
                                            (
                                                events.classified,
                                                events.settled,
                                                events.packet_errors
                                            ),
                                            (4, 2, 0)
                                        );
                                        assert!(!events.rejected);
                                    } else {
                                        assert_eq!(
                                            (events.classified, events.settled, events.ingress),
                                            (1, 0, 0)
                                        );
                                    }
                                });
                            })
                            .expect("endpoint retirement");
                        address
                    },
                    recovery::Policy::new(
                        |event| reports.push(event),
                        |failure: recovery::Recovery<'_, '_>| -> Infallible {
                            panic!("native recovery: {:?}", failure.cause())
                        },
                    ),
                )
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver retirement")
        });
        common::recovery::Teardown::new([Id::fixed::<0>()])
            .verify(reports)
            .expect("native receive retirement");
        if size == 90 {
            for byte in [b'A', b'B'] {
                let mut bytes = [0; 90];
                assert_eq!(sender.recv_from(&mut bytes).expect("reply"), (90, address));
                assert_eq!(bytes, [byte; 90]);
            }
        }
    }
}
