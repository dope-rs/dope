pub mod common;

use std::{array, convert::Infallible, net::UdpSocket, ops::ControlFlow};

use common::{driver::Scoped as _, rt::Runtime};
use dope_core::{
    driver::{
        self,
        life::{recovery, recovery::Recovery},
        result::{SubmitCause, poll},
        retained::{self, flight, flight::cancel},
        route::{Epoch, KeyTag, indices::SlotIndex},
        settings,
    },
    io::{
        event::Kind,
        socket::{Inet, install::Install, option, pending},
        transfer,
    },
};

#[test]
fn full_queue_retries_once() {
    for wait in [false, true] {
        Runtime::quic::<64, 2048>()
            .queue_layout(settings::QueueLayout::fixed::<8, 16>())
            .with_scope(
                |scope| {
                    scope.run_turn(|mut driver, turn| {
                        let sockets: [_; 8] = array::from_fn(|_| {
                            Install::bind_udp(driver.reborrow(), common::LOCALHOST, common::DATAGRAM)
                                .expect("idle datagram socket")
                                .0
                        });
                        let receives = driver
                            .storage
                            .reborrow()
                            .try_flights(flight::Need::<KeyTag<0>>::new(
                                flight::Capacity::fixed::<8>(),
                            ))
                            .expect("receive flights")
                            .slots();
                        let sends = driver
                            .storage
                            .reborrow()
                            .try_flights(flight::Need::<KeyTag<0>>::new(
                                flight::Capacity::fixed::<17>(),
                            ))
                            .expect("more send flights than native queue capacity")
                            .slots();
                        let targets = driver.driver_ref().targets::<KeyTag<0>>();
                        for (slot, socket) in (0_u16..8).zip(&sockets) {
                            let target = targets.bind(SlotIndex::from(slot), Epoch::INITIAL);
                            drop(
                                receives
                                    .recv(driver.reborrow(), socket, target)
                                    .expect("fill SQ"),
                            );
                        }
                        let target = targets.bind(SlotIndex::fixed::<8>(), Epoch::INITIAL);
                        let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                        let mut admitted = 0;
                        for _ in 0..17 {
                            let request = retained::raw::Submission::send(
                                &sockets[0],
                                transfer::View::prefix(b"retry"),
                                target,
                            );
                            // SAFETY: the static payload and exact descriptor remain
                            // retained until dispatch below or driver quiescence.
                            let sent = unsafe {
                                request.try_submit(
                                    retained::Context::new(driver.reborrow(), owner),
                                    &sends,
                                )
                            };
                            match sent {
                                Ok(flight) => {
                                    drop(flight);
                                    admitted += 1;
                                }
                                Err(cause) => {
                                    assert_eq!(cause.cause(), SubmitCause::Full);
                                    break;
                                }
                            }
                        }
                        assert!(admitted < 17, "native queue fills before flight storage");

                        let mut events = 0;
                        let result =
                            driver::Context::dispatch(&mut driver, turn.reactor(), |event, _| {
                                assert!(
                                    matches!(event.into_kind(), Kind::Send(_)),
                                    "idle receives have no CQE"
                                );
                                events += 1;
                                ControlFlow::Continue(())
                            });
                        assert!(matches!(
                            result,
                            poll::Dispatch::Ready {
                                drain: poll::Drain::Drained,
                                retained: None
                            }
                        ));
                        assert_eq!(
                            events, admitted,
                            "readiness backends retire synchronous send errors; a full receive SQ has no CQE"
                        );
                        if wait {
                            driver
                                .wait(turn.reactor(), None)
                                .expect("native capacity retry must not wait for a CQE");
                            let mut count = 0;
                            let result = driver::Context::dispatch(
                                &mut driver,
                                turn.reactor(),
                                |_, _| {
                                    count += 1;
                                    ControlFlow::Continue(())
                                },
                            );
                            assert_eq!(count, 0, "idle receives have no CQE");
                            assert!(matches!(
                                result,
                                poll::Dispatch::Ready {
                                    drain: poll::Drain::Drained,
                                    retained: None
                                }
                            ));
                            let request = retained::raw::Submission::send(
                                &sockets[0],
                                transfer::View::prefix(b"retry"),
                                target,
                            );
                            // SAFETY: the static payload and exact descriptor remain
                            // retained through driver quiescence.
                            let sent = unsafe {
                                request.try_submit(
                                    retained::Context::new(driver.reborrow(), owner),
                                    &sends,
                                )
                            };
                            drop(sent.expect("native capacity available after wait"));
                        } else {
                            assert!(matches!(
                                driver.commit(turn.reactor()),
                                Ok(poll::Commit::Pending)
                            ));
                        }
                        assert!(matches!(
                            driver.commit(turn.reactor()),
                            Ok(poll::Commit::Drained)
                        ));

                        let full = receives.recv(driver.reborrow(), &sockets[0], target);
                        assert_eq!(
                            full.err().map(|cause| cause.cause()),
                            Some(SubmitCause::Full)
                        );
                        assert!(
                            matches!(driver.commit(turn.reactor()), Ok(poll::Commit::Drained)),
                            "flight capacity exhaustion does not request native publication retry"
                        );
                    });
                },
                recovery::Policy::new(
                    |_| {},
                    |failure: Recovery<'_, '_>| -> Infallible {
                        panic!("native retirement: {:?}", failure.cause())
                    },
                ),
                |result| {
                    result
                        .as_ref()
                        .map_err(|cause| cause.cause())
                        .expect("driver lifecycle");
                },
            );
    }
}

#[test]
fn cancel_retries_once() {
    Runtime::quic::<64, 2048>()
        .queue_layout(settings::QueueLayout::fixed::<8, 8>())
        .with_scope(
            |scope| {
                scope.run_turn(|mut driver, turn| {
                    let sockets: [_; 16] = array::from_fn(|_| {
                        Install::bind_udp(driver.reborrow(), common::LOCALHOST, common::DATAGRAM)
                            .expect("idle datagram socket")
                            .0
                    });
                    let receives = driver
                        .storage
                        .reborrow()
                        .try_flights(flight::Need::<KeyTag<0>>::new(
                            flight::Capacity::fixed::<16>(),
                        ))
                        .expect("receive flights")
                        .slots();
                    let targets = driver.driver_ref().targets::<KeyTag<0>>();
                    let mut flights = Vec::with_capacity(16);
                    for (slot, socket) in (0_u16..16).zip(&sockets) {
                        if slot == 8 {
                            assert!(matches!(
                                driver.commit(turn.reactor()),
                                Ok(poll::Commit::Drained)
                            ));
                        }
                        let target = targets.bind(SlotIndex::from(slot), Epoch::INITIAL);
                        flights.push(
                            receives
                                .recv(driver.reborrow(), socket, target)
                                .expect("idle receive"),
                        );
                    }
                    let mut stopped = 0;
                    for flight in &mut flights {
                        let outcome = flight.try_stop(&mut driver).expect("cancel admission");
                        if matches!(outcome, cancel::Outcome::Deferred) {
                            break;
                        }
                        assert!(
                            matches!(outcome, cancel::Outcome::Submitted),
                            "receive has no prior terminal"
                        );
                        stopped += 1;
                    }
                    assert!(
                        stopped < flights.len(),
                        "native cancellation capacity fills"
                    );
                    let mut events = 0;
                    let result = driver.dispatch(turn.reactor(), |event, driver| {
                        let done = match event.into_kind() {
                            Kind::Recv(done) => Ok(done),
                            event => Err(event),
                        }
                        .ok()
                        .expect("cancelled receive");
                        driver.reclaim(done);
                        events += 1;
                        ControlFlow::Continue(())
                    });
                    assert_eq!(events, stopped, "idle receives produce no CQE");
                    assert!(matches!(
                        result,
                        poll::Dispatch::Ready {
                            drain: poll::Drain::Drained,
                            retained: None
                        }
                    ));
                    assert!(matches!(
                        driver.commit(turn.reactor()),
                        Ok(poll::Commit::Pending)
                    ));
                    assert!(matches!(
                        driver.commit(turn.reactor()),
                        Ok(poll::Commit::Drained)
                    ));
                    assert!(matches!(
                        flights[stopped].try_stop(&mut driver),
                        Ok(cancel::Outcome::Submitted)
                    ));
                });
            },
            recovery::Policy::new(
                |_| {},
                |failure: Recovery<'_, '_>| -> Infallible {
                    panic!("native retirement: {:?}", failure.cause())
                },
            ),
            |result| {
                result
                    .as_ref()
                    .map_err(|cause| cause.cause())
                    .expect("driver lifecycle");
            },
        );
}

#[test]
fn socket_retries_once() {
    let peer = UdpSocket::bind(common::LOCALHOST).expect("connect peer");
    let address = Inet::from_std(peer.local_addr().expect("peer address"));
    let buffer = option::Buffer::try_from(4096_usize).expect("buffer size");
    let options = option::Set::new(Some(buffer), None, None, None, None, None, None);
    for connect in [false, true] {
        Runtime::quic::<64, 2048>()
            .queue_layout(settings::QueueLayout::fixed::<8, 8>())
            .file_slots(settings::FileSlots::fixed::<0, 32>())
            .with_scope(
                |scope| {
                    scope.run_turn(|mut driver, turn| {
                        let idle: [_; 8] = array::from_fn(|_| {
                            Install::bind_udp(
                                driver.reborrow(),
                                common::LOCALHOST,
                                common::DATAGRAM,
                            )
                            .expect("idle datagram socket")
                            .0
                        });
                        let sockets: [_; 9] = array::from_fn(|_| {
                            Install::bind_udp(
                                driver.reborrow(),
                                common::LOCALHOST,
                                common::DATAGRAM,
                            )
                            .expect("socket for admission")
                            .0
                        });
                        let receives = driver
                            .storage
                            .reborrow()
                            .try_flights(flight::Need::<KeyTag<0>>::new(
                                flight::Capacity::fixed::<8>(),
                            ))
                            .expect("receive flights")
                            .slots();
                        let controls = driver
                            .storage
                            .reborrow()
                            .try_flights(flight::Need::<KeyTag<1>>::new(
                                flight::Capacity::fixed::<9>(),
                            ))
                            .expect("spare control flights")
                            .slots();
                        let targets = driver.driver_ref().targets::<KeyTag<0>>();
                        for (slot, socket) in (0_u16..8).zip(&idle) {
                            let target = targets.bind(SlotIndex::from(slot), Epoch::INITIAL);
                            drop(
                                receives
                                    .recv(driver.reborrow(), socket, target)
                                    .expect("fill SQ"),
                            );
                        }
                        let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                        let targets = driver.driver_ref().targets::<KeyTag<1>>();
                        let mut sockets = sockets.into_iter();
                        let mut started = Vec::with_capacity(8);
                        let mut applied = None;
                        let mut rejected = None;
                        for socket in sockets.by_ref() {
                            let slot = socket.index();
                            let target = targets.bind(slot, Epoch::INITIAL);
                            let result = if connect {
                                // SAFETY: address remains fixed outside the driver
                                // generation until every connection is quiescent.
                                let result = unsafe {
                                    retained::raw::Connect::new(socket.into(), &address, target)
                                        .try_connect(
                                            retained::Context::new(driver.reborrow(), owner),
                                            &controls,
                                            &options,
                                        )
                                };
                                result.map(|connect| started.push(connect))
                            } else {
                                option::Tuning::try_new(
                                    &mut driver,
                                    &controls,
                                    target,
                                    socket.into(),
                                    options,
                                )
                                .map(|tuning| {
                                    let socket = match tuning {
                                        option::Tuning::Applied(socket) => Some(socket),
                                        option::Tuning::Pending(_) => None,
                                    }
                                    .expect("receive SQ is full");
                                    applied = Some(socket);
                                })
                            };
                            if let Err(error) = result {
                                rejected = Some((slot, error));
                                break;
                            }
                            if applied.is_some() {
                                break;
                            }
                        }
                        if let Some((slot, error)) = rejected {
                            let (socket, cause) = error.into_parts();
                            assert_eq!(socket.index(), slot, "rejection retains its descriptor");
                            let error = match cause {
                                pending::Cause::Submit(error) => Ok(error),
                                pending::Cause::Option(error) => Err(error),
                            }
                            .expect("setsockopt must succeed");
                            assert_eq!(error.cause(), SubmitCause::Full);
                            let mut events = 0;
                            let result = driver.dispatch(turn.reactor(), |event, _| {
                                assert!(matches!(event.into_kind(), Kind::Connect(_)));
                                events += 1;
                                ControlFlow::Continue(())
                            });
                            assert_eq!(events, started.len(), "idle receives produce no CQE");
                            assert!(matches!(
                                result,
                                poll::Dispatch::Ready {
                                    drain: poll::Drain::Drained,
                                    retained: None
                                }
                            ));
                            assert!(matches!(
                                driver.commit(turn.reactor()),
                                Ok(poll::Commit::Pending)
                            ));
                            assert!(matches!(
                                driver.commit(turn.reactor()),
                                Ok(poll::Commit::Drained)
                            ));
                            drop(socket);
                        } else {
                            assert!(applied.is_some(), "readiness tuning applies synchronously");
                        }
                    });
                },
                recovery::Policy::new(
                    |_| {},
                    |failure: Recovery<'_, '_>| -> Infallible {
                        panic!("native retirement: {:?}", failure.cause())
                    },
                ),
                |result| {
                    result
                        .as_ref()
                        .map_err(|cause| cause.cause())
                        .expect("driver lifecycle");
                },
            );
    }
}
