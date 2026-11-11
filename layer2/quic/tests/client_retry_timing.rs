pub mod common;

use std::{cell::Cell, convert::Infallible, net::UdpSocket, time::Duration};

use common::{
    peer, recovery,
    retry::{
        BIND, ENDPOINT, Events, timing,
        writable::{Callback, Connected},
    },
    suite::peers::tls,
};
use dope::{
    core::driver::{
        life::{
            creation::Attempt,
            recovery::{Policy, Recovery},
        },
        route::Id,
    },
    runtime::{
        executor::{Executor, session},
        shutdown,
    },
};
use dope_quic::{
    client::{self, build, protocol},
    conn::Timing,
    runtime::endpoint::client::Plan,
};
use dope_test::harness::scenario::TIMEOUT;

#[test]
fn retains_retry_deadline_failure() {
    let (_, public) = peer::raw(0x6d).expect("raw identity");
    let peer = UdpSocket::bind(BIND).expect("quiet peer");
    let address = peer.local_addr().expect("peer address");
    for defer in [true, false] {
        let started = Cell::new(None);
        let closed_at = Cell::new(None);
        let failure = Cell::new(None);
        let retries = Cell::new(0);
        let closes = Cell::new(0);
        let plan = Plan::<1>::new(BIND, ENDPOINT).expect("client endpoint");
        let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
        let mut create = Attempt::new();
        let executor = Executor::new(
            &mut create,
            Events::settings(8, plan.as_ref()).expect("driver claims"),
        );
        executor
            .as_ref()
            .map_err(|cause| cause.kind())
            .expect("driver");
        let executor = executor
            .ok()
            .expect("driver admission")
            .shutdown(source)
            .expect("shutdown source");
        let mut reports = Vec::new();
        executor.run(|executor| {
            executor
                .enter_with(
                    |mut session| {
                        let (client, timeout) = {
                            let mut driver = session.driver();
                            let timeout = driver
                                .driver_ref()
                                .clock()
                                .now()
                                .try_add(Duration::from_secs(2))
                                .expect("test deadline");
                            let client = build::Builder::owned_with(
                                vec![client::EndpointSpec {
                                    addr: address,
                                    authority: tls::client(public).expect("TLS authority"),
                                }],
                                timing::Config {
                                    defer,
                                    started: &started,
                                },
                                Connected::new(|event: Result<client::SlotId, Callback<'_>>| {
                                    let Err(Callback::Close) = event else {
                                        panic!("unexpected callback: {event:?}");
                                    };
                                    closes.set(closes.get() + 1);
                                }),
                                timing::Backoff(&retries),
                                client::Config {
                                    event_budget: 1,
                                    retry_budget: 1,
                                },
                            )
                            .bind(plan, &mut driver)
                            .expect("bound client");
                            (client, timeout)
                        };
                        let mut report = session::Completion::new();
                        session
                            .with_quiesce(
                                &mut report,
                                timing::App {
                                    client,
                                    started: &started,
                                    closed_at: &closed_at,
                                    failure: &failure,
                                    stop: Some(stop),
                                    timeout,
                                },
                                TIMEOUT,
                                |app| app.run_for(TIMEOUT).map(|_| ()),
                            )
                            .expect("session completion")
                            .expect("application completion");
                    },
                    Policy::new(
                        |event| reports.push(event),
                        |failure: Recovery<'_, '_>| -> Infallible {
                            panic!("native recovery: {:?}", failure.cause())
                        },
                    ),
                )
                .as_ref()
                .map_err(|cause| cause.cause())
                .expect("driver retirement");
        });
        let (expected, began) = started.get().expect("first attempt");
        let Some(protocol::Failure::Timing {
            attempt,
            cause:
                Timing::Deadline {
                    at,
                    duration_secs,
                    duration_nanos,
                },
        }) = failure.get()
        else {
            panic!("exact retry timing failure: {:?}", failure.get());
        };
        assert_eq!(attempt, expected);
        assert_eq!(attempt.generation(), 1);
        if defer {
            assert_eq!(at, began);
        } else {
            assert!(at >= closed_at.get().expect("close time"));
        }
        assert_eq!(duration_secs, Duration::MAX.as_secs());
        assert_eq!(duration_nanos.get(), Duration::MAX.subsec_nanos());
        assert_eq!(retries.get(), 1);
        assert_eq!(closes.get(), usize::from(!defer));
        recovery::Teardown::new([Id::fixed::<1>()])
            .verify(reports)
            .expect("exact receive retirement");
    }
}
