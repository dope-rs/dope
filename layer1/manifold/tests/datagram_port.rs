pub mod common;

use std::{
    net::{Ipv4Addr, UdpSocket},
    num::NonZero,
    time::Duration,
};

use dope_core::{
    driver::{
        life::{creation::Attempt, recovery},
        route::{Epoch, FrameworkTag, KeyTag, Tag, kind, lease},
        settings,
    },
    io::socket::install,
};
use dope_manifold::{
    app::{Host, creation},
    network::datagram::{RejectCause, plan, send},
};
use dope_runtime::executor::{Executor, drive, session::Completion};
use dope_test::{
    fibers,
    harness::signals::{Gate, Progress as _},
    reclaim::Reports,
};
use o3::base::marker::ThreadBound;

use crate::common::{
    manifold::datagram::{
        Idle, handler,
        owners::{Owners, Reinstallation},
    },
    recover,
    scenario::{Runtime, app::Empty},
};

#[test]
fn binds_concurrently() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let received = Gate::new();
    let in_use = dope_core::Errno::try_from(libc::EADDRINUSE).expect("errno");
    (0..5)
        .find_map(|_| {
            let addr = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
                .expect("reserve ephemeral port")
                .local_addr()
                .expect("reserved address");
            assert_ne!(addr.port(), 0);
            let runtime = Runtime::quic::<4096, 2048>();
            let config = runtime.config();
            let executor = {
                let outcome = runtime.try_executor(&mut create);
                assert!(outcome.is_ok(), "first executor: {:?}", outcome.as_ref().err().map(|error| error.kind()));
                outcome.ok().expect("native creation")
            };
            let mut events1 =
                Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

            executor.run(|executor| {
    let result = executor.enter_with(
                |mut first| {
                    let endpoint = Idle(received.clone()).bind(addr, check_idle, &mut first.driver())?;
                    let result =
                        first.with_quiesce(&mut finish, Host::new(endpoint), Duration::from_secs(2), |_| {
                            let runtime = Runtime::quic::<4096, 2048>();
                            let mut nested = Completion::new();
                            let mut events2 = Reports::<_>::new(
                                runtime.config(),
                                [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                            );

                            {
                                let result = runtime
                                .try_executor(&mut create);
                                result
                                    .as_ref()
                                    .map_err(|error| error.kind())
                                    .expect("native creation");
                                result.ok().expect("native creation")
                            }.run(|executor| {
                                    let result = executor.enter_with(
                                    |mut second| {
                                        let endpoint = {
                                            let outcome = Idle(received.clone()).bind(addr, check_idle, &mut second.driver());
                                            assert!(
                                                outcome.is_ok(),
                                                "SO_REUSEPORT must permit a second live bind"
                                            );
                                            outcome.expect(
                                                "SO_REUSEPORT must permit a second live bind",
                                            )
                                        };
                                        assert_eq!(endpoint.local_addr(), addr);
                                        assert!(
                                            second
                                                .with_quiesce(&mut nested,
                                                    Host::new(endpoint),
                                                    Duration::from_secs(2),
                                                    |_| {}
                                                )
                                                .is_ok(),
                                            "second endpoint teardown"
                                        );
                                    },
                                    recovery::Policy::new(
                                        |event| events2.push(event),
                                        recover,
                                    ),
                                );
                                    result
                                        .as_ref()
                                        .map_err(|error| error.cause())
                                        .expect("native retirement");
                                    result.ok().expect("native retirement")
                                });
                            assert!(
                                events2.events().expect("reclaim validation").is_empty(),
                                "unexpected reclaimed operations: {events2:?}"
                            );
                        });
                    assert!(result.is_ok(), "first endpoint teardown");
                    Ok(())
                },
                recovery::Policy::new(|event| events1.push(event), recover),
            );
            assert!(
                events1.events().expect("reclaim validation").is_empty(),
                "unexpected reclaimed operations: {events1:?}"
            );

            let result = {
                let outcome = result;
                assert!(outcome.is_ok(), "first executor teardown: {:?}", outcome.as_ref().err().map(|error| error.cause()));
                outcome.ok().expect("native retirement")
            };
            match result {
                Ok(()) => Some(()),
                Err(error) => {
                    assert!(
                        matches!(error, creation::Error::Binding(dope_net::BindingError::Bootstrap(
                    install::Error::System { operation: install::Operation::Bind, fault }
                )) if fault == in_use)
                    );
                    None
                }
            }
            })
})
        .expect("ephemeral port reservation raced on every attempt");
    assert_eq!(received.hits(), 0, "idle sockets received a packet");
}

#[test]
fn finish_reuses_slot() {
    let mut finish = Completion::new();
    let config = Runtime::quic::<64, 2048>()
        .config()
        .with_domains(settings::Domain::ONE)
        .with_files(settings::FileSlots::fixed::<0, 1>())
        .with_domains(settings::Domain::fixed::<8>());
    let executor = {
        use dope_core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let outcome = Executor::new(&mut create, config);
        assert!(
            outcome.is_ok(),
            "executor creation failed: {:?}",
            outcome.as_ref().err().map(|error| error.kind())
        );
        outcome.ok().expect("native creation")
    };
    let received = Gate::new();
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                for _ in 0..8 {
                    let endpoint = {
                        let mut driver = session.driver();
                        {
                            let outcome = Idle(received.clone()).bind(
                                (Ipv4Addr::LOCALHOST, 0).into(),
                                check_idle,
                                &mut driver,
                            );
                            outcome.expect("fixed slot must be reclaimed")
                        }
                    };
                    assert!(
                        session
                            .with_quiesce(
                                &mut finish,
                                Host::new(endpoint),
                                Duration::from_secs(2),
                                |_| {}
                            )
                            .is_ok(),
                        "endpoint teardown"
                    );
                }
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.events().expect("reclaim validation").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "executor teardown");
        assert_eq!(received.hits(), 0, "idle sockets received a packet");
    });
}

#[test]
fn reinstallation_preserves_payload_identity() {
    let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("peer socket");
    let addr = peer.local_addr().expect("peer address");
    let config = Runtime::quic::<64, 2048>()
        .config()
        .with_files(settings::FileSlots::fixed::<0, 1>())
        .with_domains(settings::Domain::fixed::<3>());
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, config)
        .ok()
        .expect("native creation");
    let mut finish = Completion::new();
    let mut reports = Reports::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let send = plan::Config::new(2, 2, 1).expect("send bounds");
                let mut queued = Vec::new();
                let mut flights = Vec::new();
                {
                    let mut driver = session.driver();
                    let prepared = plan::Plan::<KeyTag<0>>::new(send)
                        .prepare(&mut driver)
                        .expect("endpoint preparation");
                    let endpoint = prepared
                        .bind(
                            (Ipv4Addr::LOCALHOST, 0).into(),
                            handler::Handler::new(Idle(Gate::new()), check_idle),
                            &mut driver,
                        )
                        .expect("endpoint bind");
                    let mut endpoint = Box::pin(endpoint);
                    for byte in [1, 2] {
                        let (_, socket) = endpoint.as_mut().split();
                        queued.push(
                            socket
                                .queue_vec(vec![byte], addr)
                                .ok()
                                .expect("unsubmitted payload"),
                        );
                    }
                }
                assert!(
                    session
                        .with_quiesce(&mut finish, Empty {}, Duration::from_secs(2), |_| {})
                        .is_ok(),
                    "uninstalled endpoint retirement"
                );

                for _ in 0..2 {
                    let settled = Gate::new();
                    let endpoint = {
                        let mut driver = session.driver();
                        plan::Plan::<KeyTag<0>>::new(send)
                            .prepare(&mut driver)
                            .expect("endpoint preparation")
                            .bind(
                                (Ipv4Addr::LOCALHOST, 0).into(),
                                handler::Handler::new(
                                    Reinstallation {
                                        addr,
                                        queued: &mut queued,
                                        flights: &mut flights,
                                        current: Owners {
                                            queued: None,
                                            flight: None,
                                        },
                                        settled: settled.clone(),
                                    },
                                    check_reinstallation,
                                ),
                                &mut driver,
                            )
                            .expect("reinstalled endpoint")
                    };
                    assert!(
                        session
                            .with_quiesce(
                                &mut finish,
                                Host::new(endpoint),
                                Duration::from_secs(2),
                                |mut app| {
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &settled, 1)
                                            .expect("send completion"),
                                        fibers::Wait::Reached
                                    );
                                },
                            )
                            .is_ok(),
                        "endpoint retirement"
                    );
                }
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "executor retirement");
    });
    reports.events().expect("reclaim validation");
}

#[test]
fn reinstallation_preserves_exhaustion() {
    let config = Runtime::quic::<64, 2048>()
        .config()
        .with_files(settings::FileSlots::fixed::<0, 1>());
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, config)
        .ok()
        .expect("native creation");
    let mut finish = Completion::new();
    let mut reports = Reports::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                {
                    let mut driver = session.driver();
                    let owner = lease::Owner::<KeyTag<0>>::try_reserve(&mut driver)
                        .expect("route reservation")
                        .commit();
                    owner.advance_to(Some(Epoch::new(NonZero::<u64>::MAX, ThreadBound::NEW)));
                    owner.release(&mut driver);
                }
                for last in [true, false] {
                    {
                        let mut driver = session.driver();
                        let send = plan::Config::new(2, 2, 1).expect("send bounds");
                        let endpoint = plan::Plan::<KeyTag<0>>::new(send)
                            .prepare(&mut driver)
                            .expect("endpoint preparation")
                            .bind(
                                (Ipv4Addr::LOCALHOST, 0).into(),
                                handler::Handler::new(Idle(Gate::new()), check_idle),
                                &mut driver,
                            )
                            .expect("endpoint bind");
                        let mut endpoint = Box::pin(endpoint);
                        let (_, mut socket) = endpoint.as_mut().split();
                        let addr = (Ipv4Addr::LOCALHOST, 9).into();
                        let first = socket.as_mut().queue_vec(vec![1], addr);
                        if last {
                            assert!(first.is_ok(), "last identity must remain usable");
                        } else {
                            assert!(matches!(
                                first.map_err(|rejected| rejected.into_parts().1),
                                Err(RejectCause::IdentityExhausted)
                            ));
                        }
                        assert!(matches!(
                            socket
                                .queue_vec(vec![2], addr)
                                .map_err(|rejected| rejected.into_parts().1),
                            Err(RejectCause::IdentityExhausted)
                        ));
                    }
                    assert!(
                        session
                            .with_quiesce(&mut finish, Empty {}, Duration::from_secs(2), |_| {})
                            .is_ok(),
                        "uninstalled endpoint retirement"
                    );
                }
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "executor retirement");
    });
    assert!(reports.events().expect("reclaim validation").is_empty());
}

#[test]
fn shutdown_reaps_slot() {
    let mut finish = Completion::new();
    let config = Runtime::quic::<64, 2048>()
        .config()
        .with_domains(settings::Domain::ONE)
        .with_files(settings::FileSlots::fixed::<0, 1>())
        .with_domains(settings::Domain::fixed::<2>());
    let executor = {
        use dope_core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let outcome = Executor::new(&mut create, config);
        assert!(
            outcome.is_ok(),
            "executor creation failed: {:?}",
            outcome.as_ref().err().map(|error| error.kind())
        );
        outcome.ok().expect("native creation")
    };
    let received = Gate::new();
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    executor.run(|executor| {
    let result = executor.enter_with(|mut session| {
        let endpoint = {
            let mut driver = session.driver();
            {
                let outcome = Idle(received.clone()).bind((Ipv4Addr::LOCALHOST, 0).into(), check_idle, &mut driver);
                outcome.expect("first endpoint bind failed")
            }
        };
        let result = session
            .with_quiesce(&mut finish, Host::new(endpoint), Duration::ZERO, |_| {});
        assert!(
            matches!(result, Err(dope_runtime::executor::session::Error::Quiesce { output: (), drive, disarm: None })
                    if drive.kind() == drive::Kind::QuiescenceTimeout)
        );

        let endpoint = {
            let mut driver = session.driver();
            {
                let outcome = Idle(received.clone()).bind((Ipv4Addr::LOCALHOST, 0).into(), check_idle, &mut driver);
                assert!(
                    outcome.is_ok(),
                    "forced endpoint must release its only fixed slot"
                );
                outcome

                    .expect("forced endpoint must release its only fixed slot")
            }
        };
        assert!(
            session.with_quiesce(&mut finish, Host::new(endpoint), Duration::from_secs(2), |_| {}).is_ok(),
            "endpoint teardown"
        );
    }, recovery::Policy::new(|event| reports.push(event), recover));
    assert!(
        reports.events().expect("reclaim validation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert!(result.is_ok(), "executor teardown");
    assert_eq!(received.hits(), 0, "idle sockets received a packet");
    });
}

fn check_idle<'d>(this: &mut Idle, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Access(received, _) => {
            received.expect("receive a valid datagram");
            this.0.hit();
        }
        event => assert!(
            matches!(event, handler::Event::Access(..)),
            "unexpected datagram callback"
        ),
    }
}

fn check_reinstallation<'d>(
    this: &mut Reinstallation<'_, 'd>,
    event: handler::Event<'_, '_, '_, '_, 'd>,
) {
    match event {
        handler::Event::Park(socket, _, _) => {
            if handler::Progress::is_runnable(this) {
                let owner = socket
                    .queue_vec(vec![3], this.addr)
                    .ok()
                    .expect("queued payload");
                assert!(!this.queued.contains(&owner), "queued identity reused");
                this.queued.push(owner);
                this.current.queued = Some(owner);
            }
        }
        handler::Event::Transfer(transfer) => {
            assert!(this.current.queued.take() == Some(transfer.queued()));
            let owner = transfer.flight();
            assert!(!this.flights.contains(&owner), "flight identity reused");
            this.flights.push(owner);
            this.current.flight = Some(owner);
        }
        handler::Event::Settle(event) => {
            let owner = this.current.flight.expect("submitted payload");
            assert!(event.owner() == send::Owner::Flight(owner));
            assert!(matches!(event.outcome(), send::Outcome::Sent(_)));
            this.settled.hit();
        }
        _ => panic!("unexpected datagram callback"),
    }
}
