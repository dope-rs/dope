pub mod common;

use std::{convert::Infallible, net, num::NonZero, time::Duration};

use common::{
    manifold::datagram::{Receive, budget, handler},
    scenario::Runtime,
};
use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
};
use dope_manifold::{
    ConfigError,
    app::creation,
    network::datagram::{packet, plan, plan::Config},
};
use dope_runtime::executor::session::Completion;
use dope_test::{fibers, harness::signals::Gate, reclaim::Reports};
use o3::buffer::{pool, storage::provided::account};

#[test]
fn retained_is_bounded() {
    for slots in [false, true] {
        let mut finish = Completion::new();
        let mut create = Attempt::new();
        let done = Gate::new();
        let runtime = Runtime::quic::<4096, 2048>();
        let bytes = runtime.config().receive().buffer_len();
        let mut reports =
            Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

        {
            let result = runtime.try_executor(&mut create);
            result
                .as_ref()
                .map_err(|error| error.kind())
                .expect("native creation");
            result.ok().expect("native creation")
        }
        .run(|executor| {
            let result = executor.enter_with(
                |mut session| {
                    let config = {
                        let outcome = Config::new(2, 4096, 2);
                        outcome.expect("valid send bounds rejected")
                    };
                    let config = if slots {
                        config.retain_slots(NonZero::<usize>::MIN)
                    } else {
                        config.retain_recv(bytes.into_nonzero().get() as usize)
                    };
                    let endpoint = {
                        let mut driver = session.driver();
                        let plan = {
                            let outcome = plan::Plan::<KeyTag<0>>::new(config).prepare(&mut driver);
                            outcome.expect("endpoint preparation failed")
                        };

                        {
                            let outcome = plan.bind(
                                (net::Ipv4Addr::LOCALHOST, 0).into(),
                                handler::Handler::new(
                                    budget::Receive {
                                        retained: None,
                                        capacity: bytes.into_nonzero().get() as usize,
                                        done: done.clone(),
                                    },
                                    check_retained,
                                ),
                                &mut driver,
                            );
                            outcome.expect("endpoint bind failed")
                        }
                    };
                    let addr = endpoint.local_addr();
                    let sender = (net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)))
                        .expect("sender bind failed");
                    assert!(matches!(sender.send_to(&[1], addr), Ok(1)));
                    assert!(matches!(sender.send_to(&[2], addr), Ok(1)));

                    ({
                        let outcome = session.with_quiesce(
                            &mut finish,
                            budget::App { endpoint },
                            Duration::from_secs(2),
                            |mut app| {
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &done, 1)
                                        .expect("progress wait"),
                                    fibers::Wait::Reached
                                );
                            },
                        );
                        assert!(outcome.is_ok(), "application teardown");
                    });
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("native retirement");
            result.ok().expect("native retirement")
        });
        assert!(
            reports
                .matches_one(&Receive::<KeyTag<0>>(Parts::new(
                    SlotIndex::ZERO,
                    Epoch::INITIAL,
                )))
                .expect("reclaim validation")
        );
    }
}

#[test]
fn slots_overflow() {
    let mut create = Attempt::new();
    let runtime = Runtime::quic::<2, 2048>();
    let mut events = Vec::new();
    {
        let result = runtime.try_executor(&mut create);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    }
    .run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let config = Config::new(1, 0, 1)
                    .expect("datagram configuration")
                    .retain_slots(NonZero::<usize>::MAX);
                let mut driver = session.driver();
                let bytes = driver.receive().buffer_len().into_nonzero().get() as usize;
                let result =
                    plan::Plan::<KeyTag<0>>::new(config).prepare::<pool::Cursor>(&mut driver);
                assert!(matches!(
                    result,
                    Err(creation::Error::Config(
                        ConfigError::DatagramReceiveCapacity {
                            slots,
                            bytes: actual,
                        }
                    )) if slots == NonZero::<usize>::MAX && actual.get() == bytes
                ));
            },
            recovery::Policy::new(|event| events.push(event), recover),
        );
        result
            .as_ref()
            .map_err(|error| error.cause())
            .expect("native retirement");
        result.ok().expect("native retirement")
    });
    assert!(events.is_empty(), "{events:?}");
}

fn check_retained<'d>(this: &mut budget::Receive<'d>, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Access(received, socket) => {
            let (_, packet) = received.expect("receive a valid datagram");

            let packet = packet.freeze();
            let retainer = socket.as_ref().retainer();
            {
                let Some(retained) = this.retained.take() else {
                    let retained = retainer
                        .try_retain(&packet)
                        .expect("first retained packet fits");
                    this.retained = Some(retained);
                    return;
                };
                assert_eq!(retained.as_ref().len(), 1);
                assert!(matches!(
                    retainer.try_retain(&packet),
                    Err(packet::Cause::Account(account::Cause::Budget(error)))
                    if { let (need, room) = error.into_parts(); (need.get(), room) == (this.capacity, 0) }
                ));
            }
            let retained = retainer
                .try_retain(&packet)
                .expect("released credit admits retained packet");
            this.retained = Some(retained);
            this.done.hit();
        }
        other => assert!(
            matches!(other, handler::Event::Access(..)),
            "unexpected datagram callback"
        ),
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
