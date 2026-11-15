pub mod common;

use std::{net, num::NonZero, time::Duration};

use common::{
    manifold::datagram::{Receive, budget, handler},
    recover,
    scenario::Runtime,
};
use dope_core::driver::{
    life::recovery,
    route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
};
use dope_manifold::{
    ConfigError,
    app::{Host, creation},
    network::datagram::{packet, plan, plan::Config},
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::signals::{Gate, LocalGate, Progress as _},
    reclaim::Reports,
};
use o3::{
    buffer::{pool, storage::provided::account},
    depot::capacity,
};

#[test]
fn backlog_recycles() {
    let received = LocalGate::new();
    let received = received.handle();
    let cleared = LocalGate::new();
    let cleared = cleared.handle();
    let runtime = Runtime::quic::<2, 2048>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    runtime.with_scope(
        |mut session| {
            let endpoint = {
                let mut driver = session.driver();
                plan::Plan::<KeyTag<0>>::new(Config::new(1, 0, 1).expect("send bounds"))
                    .prepare(&mut driver)
                    .expect("endpoint preparation")
                    .bind(
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        handler::Handler::new(
                            budget::Backlog {
                                packets: packet::Backlog::try_new(capacity::Count::new(1))
                                    .expect("backlog allocation"),
                                received,
                                cleared,
                            },
                            check_backlog,
                        ),
                        &mut driver,
                    )
                    .expect("endpoint bind")
            };
            let address = endpoint.local_addr();
            let sender = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("sender bind");
            let mut finish = Completion::new();
            session
                .with_quiesce(
                    &mut finish,
                    Host::new(endpoint),
                    Duration::from_secs(2),
                    |mut app| {
                        for byte in 0_u8..3 {
                            assert_eq!(sender.send_to(&[byte], address).expect("datagram"), 1);
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &received, u32::from(byte) + 1)
                                    .expect("receive progress"),
                                fibers::Wait::Reached
                            );
                        }
                    },
                )
                .expect("endpoint retirement");
            assert_eq!(cleared.hits(), 1, "shutdown clears an admitted packet");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(
        reports
            .matches_one(&Receive::<KeyTag<0>>(Parts::new(
                SlotIndex::ZERO,
                Epoch::INITIAL
            )))
            .expect("reclaim validation")
    );
}

fn check_backlog<'d>(
    this: &mut budget::Backlog<'_, 'd>,
    event: handler::Event<'_, '_, '_, '_, 'd>,
) {
    match event {
        handler::Event::Access(received, _) => {
            let (source, packet) = received.expect("native datagram");
            assert_eq!(
                packet.as_ref(),
                [u8::try_from(this.received.hits()).expect("fixture count")]
            );
            let pointer = packet.as_ref().as_ptr();
            match this.packets.try_push(source, packet) {
                Ok(()) => assert_eq!(this.received.hits(), 0),
                Err(rejected) => {
                    let (packet, cause) = rejected.into_parts();
                    assert_eq!((cause.used(), cause.capacity()), (1, 1));
                    assert_eq!(packet.as_ref().as_ptr(), pointer);
                    this.packets.clear();
                    this.packets
                        .try_push(source, packet)
                        .map_err(|rejected| rejected.into_parts().1)
                        .expect("released backlog slot");
                }
            }
            this.received.hit();
        }
        _ => panic!("unexpected datagram callback"),
    }
}

#[test]
fn retained_is_bounded() {
    for slots in [false, true] {
        let mut finish = Completion::new();
        let done = Gate::new();
        let runtime = Runtime::quic::<4096, 2048>();
        let bytes = runtime.config().receive().buffer_len();
        let mut reports =
            Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

        runtime.with_scope(
            |mut session| {
                let config = Config::new(2, 4096, 2).expect("valid send bounds rejected");
                let config = if slots {
                    config.retain_slots(NonZero::<usize>::MIN)
                } else {
                    config.retain_recv(bytes.into_nonzero().get() as usize)
                };
                let endpoint = {
                    let mut driver = session.driver();
                    let plan = plan::Plan::<KeyTag<0>>::new(config)
                        .prepare(&mut driver)
                        .expect("endpoint preparation failed");

                    plan.bind(
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
                    )
                    .expect("endpoint bind failed")
                };
                let addr = endpoint.local_addr();
                let sender = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0))
                    .expect("sender bind failed");
                assert!(matches!(sender.send_to(&[1], addr), Ok(1)));
                assert!(matches!(sender.send_to(&[2], addr), Ok(1)));

                let outcome = session.with_quiesce(
                    &mut finish,
                    Host::new(endpoint),
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
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
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
    let runtime = Runtime::quic::<2, 2048>();
    let mut events = Vec::new();
    runtime.with_scope(
        |mut session| {
            let config = Config::new(1, 0, 1)
                .expect("datagram configuration")
                .retain_slots(NonZero::<usize>::MAX);
            let mut driver = session.driver();
            let bytes = driver.receive().buffer_len().into_nonzero().get() as usize;
            let result = plan::Plan::<KeyTag<0>>::new(config).prepare::<pool::Cursor>(&mut driver);
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
        _ => panic!("unexpected datagram callback"),
    }
}
