pub mod common;

use std::{
    convert::Infallible,
    net::{Ipv4Addr, UdpSocket},
    time::Duration,
};

use dope_core::{
    driver::{
        life::{creation::Attempt, recovery},
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
    },
    io::datagram::{Cause, ReceiveError},
};
use dope_manifold::network::datagram::plan::{Config, Plan};
use dope_runtime::executor::session::Completion;
use dope_test::{fibers, harness::signals::Gate, reclaim::Reports};

use crate::common::{
    manifold::datagram::{expected::*, handler, *},
    scenario::Runtime,
};

#[test]
fn empty_is_packet() {
    assert_packet::<1>(&[], None);
}

#[test]
fn byte_fits_slot() {
    assert_packet::<1>(&[0xA5], None);
}

#[test]
fn truncation_resumes() {
    assert_packet::<8>(b"12345678", Some(b"123456789"));
}

#[test]
fn packet_return_resumes() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let done = Gate::new();
    let runtime = Runtime::quic::<2, 1>();
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
                    let outcome = Config::new(1, 1, 1);
                    outcome.expect("valid datagram bounds rejected")
                };
                let config = config.retain_recv(2 * bytes.into_nonzero().get() as usize);
                let receiver = {
                    let mut driver = session.driver();
                    let plan = {
                        let outcome = Plan::<KeyTag<0>>::new(config).prepare(&mut driver);
                        outcome.expect("receiver preparation failed")
                    };
                    {
                        let outcome = plan.bind(
                            (Ipv4Addr::LOCALHOST, 0).into(),
                            handler::Handler::new(
                                Exhaustion {
                                    retained: None,
                                    done: done.clone(),
                                },
                                check_exhaustion,
                            ),
                            &mut driver,
                        );
                        outcome.expect("receiver bind failed")
                    }
                };
                let addr = receiver.local_addr();
                let sender =
                    (UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))).expect("sender bind failed");
                for payload in [1, 2, 3] {
                    assert!(matches!(sender.send_to(&[payload], addr), Ok(1)));
                }

                ({
                    let outcome = session.with_quiesce(
                        &mut finish,
                        App { receiver },
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

fn assert_packet<const BYTES: u32>(payload: &'static [u8], excess: Option<&[u8]>) {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    assert_eq!(payload.len().max(1), BYTES as usize);
    let gate = Gate::new();
    let runtime = Runtime::quic::<2, BYTES>();
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
                    let outcome = Config::new(1, 0, 1);
                    outcome.expect("receive-only bounds rejected")
                };
                let receiver = {
                    let mut driver = session.driver();
                    let plan = {
                        let outcome = Plan::<KeyTag<0>>::new(config).prepare(&mut driver);
                        outcome.expect("receiver preparation failed")
                    };
                    {
                        let outcome = plan.bind(
                            (Ipv4Addr::LOCALHOST, 0).into(),
                            handler::Handler::new(
                                Expected {
                                    gate: gate.clone(),
                                    payload,
                                    failure: excess.map(|_| {
                                        ReceiveError::Truncated(Cause::Flags {
                                            actual: libc::MSG_TRUNC as u32,
                                            payload: BYTES as usize,
                                            stored: BYTES as usize,
                                        })
                                    }),
                                },
                                check_expected,
                            ),
                            &mut driver,
                        );
                        outcome.expect("receiver bind failed")
                    }
                };
                let addr = receiver.local_addr();
                let sender =
                    (UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))).expect("sender bind failed");
                if let Some(bytes) = excess {
                    assert!(matches!(sender.send_to(bytes, addr), Ok(len) if len == bytes.len()));
                }
                assert!(matches!(sender.send_to(payload, addr), Ok(len) if len == payload.len()));

                ({
                    let outcome = session.with_quiesce(
                        &mut finish,
                        App { receiver },
                        Duration::from_secs(2),
                        |mut app| {
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &gate, 1)
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

fn check_expected<'d>(this: &mut Expected<'_>, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Access(received, _) => {
            let (_, packet) = match received {
                Ok(packet) => packet,
                Err(error) => {
                    let expected = this.failure.take().expect("only the oversized frame fails");
                    assert_eq!(error, expected);
                    return;
                }
            };
            assert!(
                this.failure.is_none(),
                "the rejected frame must be reported before the next packet"
            );
            assert_eq!(packet.as_ref(), this.payload);
            this.gate.hit();
        }
        other => assert!(
            matches!(other, handler::Event::Access(..)),
            "unexpected datagram callback"
        ),
    }
}

fn check_exhaustion<'d>(this: &mut Exhaustion<'d>, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Access(received, socket) => {
            let (_, packet) = received.expect("receive a valid datagram");

            {
                match packet.as_ref() {
                    [1] => {
                        assert!(this.retained.is_none());
                        let retained = socket
                            .as_ref()
                            .retainer()
                            .try_retain(&packet.freeze())
                            .expect("first receive buffer remains retained");
                        this.retained = Some(retained);
                    }
                    bytes => {
                        assert!(matches!(bytes, [2] | [3]), "unexpected exhaustion packet");
                        this.retained
                            .as_ref()
                            .expect("the first packet remains retained");
                        if bytes == [3] {
                            this.done.hit();
                        }
                    }
                }
            }
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
