use std::{
    convert::Infallible,
    io::Read as _,
    net::TcpStream,
    ops::ControlFlow,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{
        life::recovery,
        result::poll,
        retained::flight,
        route::{Epoch, KeyTag, indices::SlotIndex, kind},
        schedule::ready,
        settings,
    },
    io::{
        event::{Kind, batch::Flow, net::accept::Outcome},
        socket::{Listener, install::Install},
    },
};
use dope_test::scope::Error;

use crate::common::{rt::Runtime, wake::Ready as _};

pub mod common;

/// Reuses the native slot before host close dispatch and rejects stale readiness.
#[test]
fn multishot_slot_reuse() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<1, 1>())
        .with_scope(
            |scope| {
                let mut driver = scope.context();
                let target = driver
                    .driver_ref()
                    .targets::<KeyTag<0, kind::Accept>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL);
                let listener = Listener::bind(common::LOCALHOST).expect("listener");
                let (listener, address) =
                    Install::adopt_bound(driver.reborrow(), listener).expect("install listener");
                let flights = driver
                    .storage
                    .reborrow()
                    .try_flights(flight::Need::<KeyTag<0, kind::Accept>>::new(
                        flight::Capacity::fixed::<1>(),
                    ))
                    .expect("accept storage")
                    .slots();
                let accept = flights
                    .accept(
                        driver.reborrow(),
                        &listener,
                        target.operation::<kind::Accept>(),
                    )
                    .expect("multishot accept");
                let mut previous: Option<ready::Handle<'_>> = None;

                for _ in 0..8 {
                    let mut peer = TcpStream::connect(address).expect("connect peer");
                    peer.set_read_timeout(Some(Duration::from_secs(1)))
                        .expect("peer deadline");
                    let started = Instant::now();
                    let mut accepted = None;
                    while accepted.is_none() {
                        assert!(
                            started.elapsed() < Duration::from_secs(1),
                            "accept deadline"
                        );
                        scope.with_driver(|mut driver, mut schedule| {
                            let mut turn = schedule.begin();
                            driver
                                .wait(turn.reactor(), Some(Duration::from_millis(50)))
                                .expect("accept progress");
                            let dispatch = driver.dispatch(turn.reactor(), |event, _| {
                                let done = match event.into_kind() {
                                    Kind::Accept(done) => Some(done),
                                    _ => None,
                                }
                                .expect("accept event");
                                let (token, flow, outcome) = done.into_parts();
                                assert!(accept.matches(token));
                                assert_eq!(flow, Flow::More);
                                let slot = match outcome {
                                    Outcome::Accepted(slot) => Ok(slot),
                                    Outcome::Failed(error) => Err(error),
                                }
                                .expect("accepted slot");
                                assert!(accepted.replace(slot.bind()).is_none());
                                ControlFlow::Continue(())
                            });
                            assert!(matches!(
                                dispatch,
                                poll::Dispatch::Ready { retained: None, .. }
                            ));
                        });
                    }
                    let fd = accepted.expect("accepted descriptor");
                    assert_eq!(fd.index(), SlotIndex::ZERO);
                    let current = fd.ready_handle();
                    scope.with_driver(|mut driver, mut schedule| {
                        let mut turn = schedule.begin();
                        let ready = driver
                            .driver_ref()
                            .targets::<KeyTag<1>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL)
                            .dispatch();
                        current.set_target(ready);
                        if let Some(stale) = previous {
                            stale.activate();
                            assert_eq!(turn.ready_round(|_| {}), 0);
                        }
                        current.activate();
                        assert_eq!(turn.ready_round(|token| assert!(ready.matches(token))), 1);
                        fd.close(&mut driver);
                        assert!(matches!(
                            driver.commit(turn.reactor()),
                            Ok(poll::Commit::Drained)
                        ));
                        driver
                            .wait(turn.reactor(), Some(Duration::from_millis(50)))
                            .expect("close progress");
                    });
                    previous = Some(current);
                    assert_eq!(peer.read(&mut [0_u8; 1]).expect("closed peer"), 0);
                }
                scope.quiesce();
                drop(accept);
                listener.close(&mut scope.context());
            },
            recovery::Policy::new(
                |_| {},
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("native retirement: {:?}", failure.cause())
                },
            ),
            |result| {
                result
                    .as_ref()
                    .map_err(Error::cause)
                    .expect("driver lifecycle");
            },
        );
}
