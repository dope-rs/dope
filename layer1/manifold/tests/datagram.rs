pub mod common;

use std::{convert::Infallible, net, num::NonZero, time::Duration};

use common::{
    manifold::datagram::{Receive, budget, group, handler, senders::Busy},
    scenario::Runtime,
};
use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table},
};
use dope_manifold::{
    ConfigError,
    network::datagram::{
        self,
        plan::{Config, Plan},
        send::Outcome,
    },
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    alloc::Tracker,
    harness::signals::{Gate, Progress as _},
    reclaim::Reports,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn send_bounds_progress() {
    assert!(matches!(
        Config::new(0, 1, 1),
        Err(ConfigError::DatagramPendingZero)
    ));
    let route = table::Capacity::try_from(usize::MAX).err();
    assert!(
        matches!(Config::new(usize::MAX, 1, 1), Err(ConfigError::DatagramPendingCapacity(error)) if Some(error) == route)
    );
    assert!(matches!(
        Config::new(1, 1, 0),
        Err(ConfigError::DatagramFlightZero)
    ));
    assert!(
        matches!(Config::new(1, 1, usize::MAX), Err(ConfigError::DatagramFlightCapacity(error)) if Some(error) == route)
    );

    assert!(Config::new(2, 3, 4).is_ok());
}

#[test]
fn shares_one_credit() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let peer = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("peer socket");
    peer.set_nonblocking(true).expect("nonblocking peer");
    let handled = Gate::new();
    let received = Gate::new();
    let settled = Gate::new();
    let rejected = Gate::new();
    let runtime = Runtime::quic::<64, 2048>();
    let mut reports = Reports::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let created = runtime.try_executor(&mut create);
    created
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("native creation");
    created.ok().expect("native creation").run(|executor| {
        let closed = executor.enter_with(
            |mut scope| {
                let config = Config::new(1, 3, 1).expect("single send slot");
                let mut driver = scope.driver();
                let endpoint = Plan::<KeyTag<0>>::new(config)
                    .prepare(&mut driver)
                    .expect("endpoint preparation")
                    .bind(
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        handler::Handler::new(
                            Busy {
                                addr: peer.local_addr().expect("peer address"),
                                pending: [1, 2, 3].map(|byte| Some(vec![byte])),
                                next: 0,
                                handled: handled.clone(),
                                received: received.clone(),
                                settled: settled.clone(),
                                rejected: rejected.clone(),
                            },
                            busy,
                        ),
                        &mut driver,
                    )
                    .expect("endpoint bind");
                let address = endpoint.local_addr();
                assert_eq!(peer.send_to(&[9], address).expect("initial packet"), 1);
                let result = scope.with_quiesce(
                    &mut finish,
                    budget::App { endpoint },
                    Duration::from_secs(2),
                    |mut app| {
                        let ((), allocs) = Tracker::usage(|| {
                            let mut bytes = [0; 2];
                            for expected in [1, 2, 3] {
                                let (length, source, left) = app
                                    .drive_for(
                                        Duration::from_secs(2),
                                        group::Read {
                                            socket: &peer,
                                            bytes: &mut bytes,
                                            credits: NonZero::<usize>::MIN,
                                        },
                                    )
                                    .expect("progress under one credit")
                                    .expect("native datagram");
                                assert_eq!(
                                    (&bytes[..length], source, left),
                                    (&[expected][..], address, 1)
                                );
                            }
                        });
                        assert_eq!(allocs, (0, 0), "packet path reuses owned storage");
                        assert_eq!(received.hits(), 1);
                        assert!(settled.hits() >= 2);
                        assert!(handled.hits() >= 3);
                        assert!(rejected.hits() >= 1, "producer retried the full queue");
                    },
                );
                assert!(result.is_ok(), "endpoint quiescence");
            },
            recovery::Policy::new(
                |event| reports.push(event),
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("native recovery: {:?}", failure.cause())
                },
            ),
        );
        closed
            .as_ref()
            .map_err(|cause| cause.cause())
            .expect("native retirement");
    });
    assert_eq!(settled.hits(), 3);
    assert!(
        reports
            .matches_one(&Receive::<KeyTag<0>>(table::Parts::new(
                SlotIndex::ZERO,
                Epoch::INITIAL,
            )))
            .expect("reclaim validation")
    );
}

/// Retries the one-slot queue while consuming every granted producer credit.
fn busy<'d>(state: &mut Busy, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Packet(packet) => {
            let (source, packet) = packet.expect("valid input");
            assert_eq!((source, packet.as_ref()), (state.addr, &[9][..]));
            state.received.hit();
        }
        handler::Event::Settle(event) => {
            assert!(matches!(event.outcome(), Outcome::Sent(bytes) if bytes.into_raw() == 1));
            state.settled.hit();
        }
        handler::Event::Park(mut socket, _, work) => {
            while let Some(permit) = work.permit() {
                permit.run(|| {
                    state.handled.hit();
                    let attempts = if state.next == 0 { 2 } else { 1 };
                    for _ in 0..attempts {
                        if state.next == state.pending.len()
                            || state.next == 2
                                && (state.received.hits() == 0 || state.settled.hits() < 2)
                        {
                            break;
                        }
                        let payload = state.pending[state.next].take().expect("pending owner");
                        match socket.as_mut().queue_vec(payload, state.addr) {
                            Ok(_) => state.next += 1,
                            Err(rejected) => {
                                let (payload, cause) = rejected.into_parts();
                                assert_eq!(cause, datagram::RejectCause::QueueFull);
                                state.pending[state.next] = Some(payload);
                                state.rejected.hit();
                                break;
                            }
                        }
                    }
                });
            }
        }
        event => assert!(
            matches!(
                event,
                handler::Event::Packet(_) | handler::Event::Settle(_) | handler::Event::Park(..)
            ),
            "unexpected datagram callback"
        ),
    }
}
