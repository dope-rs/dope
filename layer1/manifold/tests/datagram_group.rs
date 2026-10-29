pub mod common;

use std::{convert::Infallible, net, num::NonZero, time::Duration};

use common::{
    manifold::datagram::{
        Receive,
        group::{self, State},
    },
    scenario::Runtime,
};
use dope_core::{
    driver::{
        life::{creation::Attempt, recovery},
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
    },
    io,
};
use dope_manifold::network::datagram::{group::plan::Plan, plan::Config, send::Outcome};
use dope_runtime::executor::{
    drive,
    session::{self, Completion},
};
use dope_test::{
    fibers,
    harness::signals::{Gate, Progress as _},
    reclaim::{Matches as _, Reports},
};

#[test]
fn routes_work_budget() {
    for credits in [
        NonZero::<usize>::MIN,
        NonZero::<usize>::MIN.saturating_add(3),
    ] {
        let mut finish = Completion::new();
        let mut create = Attempt::new();
        let client = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("client socket");
        let upstream =
            net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("upstream socket");
        client.set_nonblocking(true).expect("nonblocking client");
        upstream
            .set_nonblocking(true)
            .expect("nonblocking upstream");
        let received = Gate::new();
        let sent = Gate::new();
        let dropped = Gate::new();
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
                    let config = Config::new(4, 16_384, 4).expect("send bounds");
                    let plan =
                        Plan::<KeyTag<0>>::try_new(config, NonZero::<u32>::MIN.saturating_add(1))
                            .expect("two socket domain");
                    let endpoint = plan
                    .bind(
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        group::Handler::new(
                            State {
                                client: client.local_addr().expect("client address"),
                                upstream: upstream.local_addr().expect("upstream address"),
                                received: received.clone(),
                            },
                            |state, received, source, mut access| {
                                let (address, packet) = received.expect("valid datagram");
                                let (slot, dest) = if source == SlotIndex::ZERO {
                                    assert_eq!(
                                        (address, packet.as_ref()),
                                        (state.client, &[1, 2][..])
                                    );
                                    (SlotIndex::fixed::<1>(), state.upstream)
                                } else {
                                    assert_eq!(source, SlotIndex::fixed::<1>());
                                    assert_eq!(
                                        (address, packet.as_ref()),
                                        (state.upstream, &[3, 4][..])
                                    );
                                    (SlotIndex::ZERO, state.client)
                                };
                                assert!(access.queue_packet(packet, slot, dest).is_ok());
                                state.received.hit();
                            },
                            |outcome| {
                                assert!(
                                    matches!(outcome, Outcome::Sent(bytes) if bytes.into_raw() == 2)
                                );
                                sent.hit();
                            },
                            dropped.clone(),
                        ),
                        &mut scope.driver(),
                    )
                    .expect("group bind");
                    let front = endpoint.local_addr(SlotIndex::ZERO).expect("front socket");
                    let member = endpoint
                        .local_addr(SlotIndex::fixed::<1>())
                        .expect("member socket");
                    assert_ne!(front, member);
                    for _ in 0..4 {
                        client.send_to(&[1, 2], front).expect("request burst");
                    }
                    let result = scope.with_quiesce(
                        &mut finish,
                        group::App { endpoint },
                        Duration::from_secs(2),
                        |mut app| {
                            let mut bytes = [0; 8];
                            for _ in 0..4 {
                                let (length, source, left) = app
                                    .drive_for(
                                        Duration::from_secs(2),
                                        group::Read {
                                            socket: &upstream,
                                            bytes: &mut bytes,
                                            credits,
                                        },
                                    )
                                    .expect("forward under bounded credits")
                                    .expect("upstream receives");
                                assert_eq!(
                                    (&bytes[..length], source, left),
                                    (&[1, 2][..], member, credits.get())
                                );
                            }
                            for _ in 0..4 {
                                upstream.send_to(&[3, 4], member).expect("reply burst");
                            }
                            for _ in 0..4 {
                                let (length, source, left) = app
                                    .drive_for(
                                        Duration::from_secs(2),
                                        group::Read {
                                            socket: &client,
                                            bytes: &mut bytes,
                                            credits,
                                        },
                                    )
                                    .expect("reply under bounded credits")
                                    .expect("client receives");
                                assert_eq!(
                                    (&bytes[..length], source, left),
                                    (&[3, 4][..], front, credits.get())
                                );
                            }
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &sent, 8)
                                    .expect("send completions"),
                                fibers::Wait::Reached,
                            );
                        },
                    );
                    assert!(result.is_ok(), "group quiescence");
                },
                recovery::Policy::new(
                    |event| {
                        assert_eq!(dropped.hits(), 0, "group owner retained during recovery");
                        reports.push(event);
                    },
                    recover,
                ),
            );
            closed
                .as_ref()
                .map_err(|cause| cause.cause())
                .expect("native retirement");
        });
        assert_eq!((received.hits(), sent.hits(), dropped.hits()), (8, 8, 1));
        let events = reports.events().expect("reclaim observations");
        assert_eq!(events.len(), 2);
        for slot in [SlotIndex::ZERO, SlotIndex::fixed::<1>()] {
            let expected = Receive::<KeyTag<0>>(Parts::new(slot, Epoch::INITIAL));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| expected.matches(event))
                    .count(),
                1
            );
        }
    }
}

#[test]
fn reports_native_failure() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let client = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("client socket");
    let received = Gate::new();
    let sent = Gate::new();
    let dropped = Gate::new();
    let expected = dope_core::Errno::try_from(libc::EAFNOSUPPORT).expect("address family error");
    let runtime = Runtime::quic::<64, 2048>();
    let created = runtime.try_executor(&mut create);
    created
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("native creation");
    created.ok().expect("native creation").run(|executor| {
        let closed = executor.enter_with(
            |mut scope| {
                let config = Config::new(1, 4096, 1).expect("single send slot");
                let plan =
                    Plan::<KeyTag<0>>::try_new(config, NonZero::<u32>::MIN.saturating_add(1))
                        .expect("two socket domain");
                let endpoint = plan
                    .bind(
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        group::Handler::new(
                            State {
                                client: client.local_addr().expect("client address"),
                                upstream: (net::Ipv6Addr::LOCALHOST, 9).into(),
                                received: received.clone(),
                            },
                            |state, received, source, mut access| {
                                let (address, packet) = received.expect("valid datagram");
                                assert_eq!((source, address), (SlotIndex::ZERO, state.client));
                                assert!(
                                    access
                                        .queue_packet(
                                            packet,
                                            SlotIndex::fixed::<1>(),
                                            state.upstream,
                                        )
                                        .is_ok()
                                );
                                state.received.hit();
                            },
                            |outcome| {
                                assert!(
                                    matches!(outcome, Outcome::Failed(error) if error == expected)
                                );
                                sent.hit();
                            },
                            dropped.clone(),
                        ),
                        &mut scope.driver(),
                    )
                    .expect("IPv4 group bind");
                let front = endpoint.local_addr(SlotIndex::ZERO).expect("front socket");
                client.send_to(&[1], front).expect("request");
                let result = scope.with_quiesce(
                    &mut finish,
                    group::App { endpoint },
                    Duration::from_secs(2),
                    |mut app| {
                        assert_eq!(
                            fibers::TEST
                                .try_wait(&mut app, &sent, 1)
                                .expect("native send failure"),
                            fibers::Wait::Reached,
                        );
                    },
                );
                assert!(result.is_ok(), "group quiescence");
            },
            recovery::Policy::new(|_| {}, recover),
        );
        closed
            .as_ref()
            .map_err(|cause| cause.cause())
            .expect("native retirement");
    });
    assert_eq!((received.hits(), sent.hits(), dropped.hits()), (1, 1, 1));
}

#[test]
fn forced_finish_retains() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let client = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("client socket");
    let upstream = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("upstream socket");
    let received = Gate::new();
    let sent = Gate::new();
    let dropped = Gate::new();
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
                let config = Config::new(2, 4096, 1).expect("send bounds");
                let plan =
                    Plan::<KeyTag<0>>::try_new(config, NonZero::<u32>::MIN.saturating_add(1))
                        .expect("two socket domain");
                let endpoint = plan
                    .bind(
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        (net::Ipv4Addr::LOCALHOST, 0).into(),
                        group::Handler::new(
                            State {
                                client: client.local_addr().expect("client address"),
                                upstream: upstream.local_addr().expect("upstream address"),
                                received: received.clone(),
                            },
                            |state, received, source, mut access| {
                                let (address, packet) = received.expect("valid datagram");
                                assert_eq!(source, SlotIndex::ZERO);
                                assert_eq!((address, packet.as_ref()), (state.client, &[1, 2][..]));
                                assert!(
                                    access
                                        .queue_packet(
                                            packet,
                                            SlotIndex::fixed::<1>(),
                                            state.upstream
                                        )
                                        .is_ok()
                                );
                                state.received.hit();
                            },
                            |_| sent.hit(),
                            dropped.clone(),
                        ),
                        &mut scope.driver(),
                    )
                    .expect("group bind");
                let front = endpoint.local_addr(SlotIndex::ZERO).expect("front socket");
                client.send_to(&[1, 2], front).expect("request owner");
                let result = scope.with_quiesce(
                    &mut finish,
                    group::App { endpoint },
                    Duration::ZERO,
                    |mut app| {
                        assert_eq!(
                            fibers::TEST
                                .try_wait(&mut app, &received, 1)
                                .expect("forward admission"),
                            fibers::Wait::Reached,
                        );
                    },
                );
                let error = result.expect_err("forced quiescence timeout");
                assert!(matches!(error,
                    session::Error::Quiesce { output: (), drive, disarm: None }
                        if drive.kind() == drive::Kind::QuiescenceTimeout
                ));
            },
            recovery::Policy::new(
                |event| {
                    assert_eq!(
                        dropped.hits(),
                        0,
                        "queued packet and descriptors retain group owner"
                    );
                    reports.push(event);
                },
                recover,
            ),
        );
        closed
            .as_ref()
            .map_err(|cause| cause.cause())
            .expect("native retirement");
    });
    assert_eq!((received.hits(), sent.hits(), dropped.hits()), (1, 0, 1));
    let mut receives = Vec::new();
    let mut sends = 0;
    for event in reports.events().expect("native reclamation") {
        match event {
            io::Reclaimed::Recv { echo, result, flow } => {
                assert_eq!(*result, Err(dope_core::Errno::CANCELED));
                assert!(matches!(flow, io::event::batch::Flow::Last));
                receives.push(echo);
            }
            io::Reclaimed::Send { result, .. } => {
                assert_eq!(result.expect("retained packet send").into_raw(), 2);
                sends += 1;
            }
            other => assert!(
                matches!(
                    other,
                    io::Reclaimed::Recv { .. } | io::Reclaimed::Send { .. }
                ),
                "unexpected native reclamation: {other:?}"
            ),
        }
    }
    assert_eq!((receives.len(), sends), (2, 1));
    assert_ne!(
        receives[0], receives[1],
        "each socket retains a distinct native receive"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
