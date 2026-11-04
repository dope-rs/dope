pub mod common;

use std::{mem, net, time::Duration};

use common::{
    manifold::datagram::{
        Receive,
        budget::{
            Packet,
            capacity::{Capacity, Stage},
        },
        handler,
    },
    recover,
    scenario::Runtime,
};
use dope_core::driver::{
    life::recovery,
    route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
};
use dope_manifold::{
    app::Host,
    network::datagram::{self, plan, plan::Config},
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::signals::{Gate, Progress as _},
    reclaim::Reports,
};
use o3::buffer::{Residency as _, storage::NonEmpty};

#[test]
fn charges_complete_slot() {
    let mut finish = Completion::new();
    const PAYLOAD_BYTES: u32 = 2048;

    let rejected = Gate::new();
    let runtime = Runtime::quic::<4096, PAYLOAD_BYTES>();
    let bytes = runtime.config().receive().buffer_len();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |mut session| {
            let config = Config::new(1, bytes.into_nonzero().get() as usize - 1, 1)
                .expect("valid send bounds rejected");
            let endpoint = {
                let mut driver = session.driver();
                let plan = plan::Plan::<KeyTag<0>>::new(config)
                    .prepare(&mut driver)
                    .expect("endpoint preparation failed");
                plan.bind(
                    (net::Ipv4Addr::LOCALHOST, 0).into(),
                    handler::Handler::new(
                        Packet {
                            rejected: rejected.clone(),
                        },
                        check_packet,
                    ),
                    &mut driver,
                )
                .expect("endpoint bind failed")
            };
            let addr = endpoint.local_addr();
            let sender =
                net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("sender bind failed");
            assert!(matches!(sender.send_to(&[], addr), Ok(0)));

            let outcome = session.with_quiesce(
                &mut finish,
                Host::new(endpoint),
                Duration::from_secs(2),
                |mut app| {
                    assert_eq!(
                        fibers::TEST
                            .try_wait(&mut app, &rejected, 1)
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

fn check_packet<'d>(this: &mut Packet, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Access(received, mut socket) => {
            let (source, packet) = received.expect("receive a valid datagram");

            assert_eq!(packet.as_ref(), []);
            let rejected = socket
                .as_mut()
                .queue_packet(packet, source)
                .err()
                .expect("a packet retains its complete receive slot");
            let (packet, cause) = rejected.into_parts();
            assert!(matches!(cause, datagram::RejectCause::Resident { .. }));
            assert_eq!(packet.as_ref(), []);
            this.rejected.hit();
        }
        _ => panic!("unexpected datagram callback"),
    }
}

#[test]
fn completion_releases() {
    let mut finish = Completion::new();
    let receiver =
        net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("receiver bind failed");
    let addr = receiver.local_addr().expect("receiver address unavailable");
    let mut first = Vec::with_capacity(4096);
    first.push(1);
    let mut second = Vec::with_capacity(4096);
    second.push(2);
    let bytes = first.capacity().max(second.capacity());
    assert!(first.len() + second.len() <= bytes);
    assert!(first.capacity() + second.capacity() > bytes);
    let second = NonEmpty::<Vec<u8>>::try_new(second).expect("nonempty second datagram");

    let released = Gate::new();
    let received = Gate::new();
    let runtime = Runtime::quic::<4096, 2048>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |mut session| {
            let config = Config::new(2, bytes, 2).expect("send bounds must be valid");
            let endpoint = {
                let mut driver = session.driver();
                let plan = plan::Plan::<KeyTag<0>>::new(config)
                    .prepare(&mut driver)
                    .expect("endpoint preparation failed");
                plan.bind(
                    (net::Ipv4Addr::LOCALHOST, 0).into(),
                    handler::Handler::new(
                        Capacity {
                            addr,
                            stage: Stage::Initial(first, second),
                            released: released.clone(),
                            received: received.clone(),
                        },
                        check_capacity,
                    ),
                    &mut driver,
                )
                .expect("endpoint bind failed")
            };

            let outcome = session.with_quiesce(
                &mut finish,
                Host::new(endpoint),
                Duration::from_secs(2),
                |mut app| {
                    assert_eq!(
                        fibers::TEST
                            .try_wait(&mut app, &released, 1)
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

    assert_eq!(received.hits(), 0, "send budget fixture received a packet");
}

fn check_capacity<'d>(this: &mut Capacity, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Access(received, _) => {
            received.expect("receive a valid datagram");
            this.received.hit();
        }
        handler::Event::Park(mut socket, _, work) => {
            let Some(permit) = work.permit() else {
                return;
            };
            permit.run(|| {
                let stage = mem::replace(&mut this.stage, Stage::Done);
                match stage {
                    Stage::Initial(first, second) => {
                        let outcome = socket.as_mut().queue_vec(first, this.addr);
                        assert!(
                            outcome.is_ok(),
                            "first datagram fits resident budget: {:?}",
                            outcome.as_ref().err().map(|error| error.cause()),
                        );
                        let owner = second.as_ref().as_ptr();
                        let resident = second.resident();
                        let result = socket.as_mut().queue_vec(second, this.addr);
                        let rejected = result
                            .err()
                            .expect("two resident capacities exceed the bound");
                        let (second, cause) = rejected.into_parts();
                        assert_eq!(second.as_ref().as_ptr(), owner);
                        assert_eq!(second.as_ref(), [2]);
                        assert_eq!(second.resident(), resident);
                        assert!(matches!(cause, datagram::RejectCause::Resident {
                        required, available: 0,
                    } if required.get() == resident));
                        this.stage = Stage::Waiting(second);
                    }
                    Stage::Waiting(payload) => {
                        let owner = payload.as_ref().as_ptr();
                        let resident = payload.resident();
                        if let Err(rejected) = socket.as_mut().queue_vec(payload, this.addr) {
                            let (payload, cause) = rejected.into_parts();
                            assert_eq!(payload.as_ref().as_ptr(), owner);
                            assert_eq!(payload.as_ref(), [2]);
                            assert_eq!(payload.resident(), resident);
                            assert!(matches!(
                                cause,
                                datagram::RejectCause::Resident { .. }
                                    | datagram::RejectCause::QueueFull
                            ));
                            this.stage = Stage::Waiting(payload);
                            return;
                        }
                        this.released.hit();
                    }
                    stage => this.stage = stage,
                }
            });
        }
        _ => panic!("unexpected datagram callback"),
    }
}
