pub mod common;

use std::{
    convert::Infallible,
    net::{Ipv4Addr, UdpSocket},
    time::Duration,
};

use dope_core::{
    driver::{
        life::{creation::Attempt, recovery},
        route::{FrameworkTag, Tag, kind},
        settings,
    },
    io::socket::install,
};
use dope_manifold::app::{Host, creation};
use dope_runtime::executor::{Executor, drive, session::Completion};
use dope_test::{
    harness::signals::{Gate, Progress as _},
    reclaim::Reports,
};

use crate::common::{
    manifold::datagram::{Idle, handler},
    scenario::Runtime,
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
