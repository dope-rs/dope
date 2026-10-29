use std::convert::Infallible;

use dope_core::{
    driver,
    driver::life::recovery,
    io::{event::Kind, fd::handles::Descriptor, socket::install::Install},
};
use dope_test::scope::Error;

use crate::common::driver::{Scoped as _, support::Drain as _};

pub mod common;

use std::{
    array,
    env::consts,
    mem,
    net::{Ipv4Addr, UdpSocket},
    num,
    ops::ControlFlow,
    time::{Duration, Instant},
};

use dope_core::driver::{
    result::poll::Dispatch,
    route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind},
    schedule,
    schedule::app::batch::{Reserved, raw::Settle as _},
    settings,
};
use dope_test::reclaim::Reports;

use crate::common::rt::Runtime;

const TWO: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(1);
const FOUR: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(3);
const FULL: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(schedule::LIMIT - 1);
const NEXT: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(schedule::LIMIT);
const OVER: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(schedule::LIMIT + 1);

/// Returns unused resume quota before dispatching queued terminal completions.
#[test]
fn resume_refunds_delivery() {
    const COUNT: usize = 200;
    let runtime = Runtime::quic::<2, 8>().queue_layout(settings::QueueLayout::fixed::<256, 256>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind shutdown peer");
            let address = peer.local_addr().expect("shutdown peer address");
            let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind shutdown socket");
            socket.connect(address).expect("connect shutdown socket");
            scope
                .with_flights::<KeyTag<7>, COUNT>(|mut driver, turn, flights| {
                    let (socket, _) = Install::adopt_udp(driver.reborrow(), socket.into())
                        .expect("adopt shutdown socket");
                    let targets = array::from_fn::<_, COUNT, _>(|index| {
                        driver
                            .driver_ref()
                            .targets::<KeyTag<7>>()
                            .bind(SlotIndex::from(index as u16), Epoch::INITIAL)
                    });
                    for target in &targets {
                        drop(
                            flights
                                .shutdown(driver.reborrow(), &socket, *target)
                                .expect("queue terminal shutdown"),
                        );
                    }

                    let deadline = Instant::now() + Duration::from_secs(1);
                    let mut seen = [false; COUNT];
                    let mut total = 0;
                    loop {
                        let left = deadline
                            .checked_duration_since(Instant::now())
                            .expect("terminal shutdown completion deadline");
                        assert!(
                            driver::Context::wait(&mut driver, turn.reactor(), Some(left)).is_ok(),
                            "publish terminal shutdown completions"
                        );
                        let mut count = 0;
                        let result =
                            driver::Context::dispatch(&mut driver, turn.reactor(), |event, _| {
                                let done = match event.into_kind() {
                                    Kind::Send(done) => Ok(done),
                                    event => Err(mem::discriminant(&event)),
                                }
                                .expect("shutdown completion");
                                let index = targets
                                    .iter()
                                    .position(|target| {
                                        target.operation::<kind::Send>().matches(done.token())
                                    })
                                    .expect("shutdown completion retains its target");
                                assert!(!mem::replace(&mut seen[index], true));
                                let valid = match done.outcome() {
                                    Ok(length) => length.into_raw() == 0,
                                    Err(errno) => {
                                        index != 0 && errno.code().get() == libc::ENOTCONN
                                    }
                                };
                                assert!(valid, "shutdown success or repeated shutdown failure");
                                count += 1;
                                ControlFlow::Continue(())
                            });
                        assert!(matches!(result, Dispatch::Ready { retained: None, .. }));
                        if consts::OS == "macos" {
                            assert_eq!(count, COUNT);
                        }
                        total += count;
                        if total == COUNT {
                            break;
                        }
                    }
                    assert!(seen.into_iter().all(|seen| seen));
                    Descriptor::from(socket).close(&mut driver);
                })
                .expect("flight fixture");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reserve_is_atomic() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            scope.with_driver(|_, mut turn| {
                {
                    let active = turn.begin();
                    assert!(matches!(
                        Reserved::reserve(active.turn().app(), OVER, false),
                        schedule::Capacity::Exhausted
                    ));
                    let observed = Reserved::reserve(active.turn().app(), FULL, false);
                    assert!(
                        matches!(&observed, schedule::Capacity::Available(_)),
                        "full batch minus its first item"
                    );
                    if let schedule::Capacity::Available(batch) = observed {
                        let work = active.turn().app();
                        assert!(
                            work.permit().is_some(),
                            "only unreserved work remains available"
                        );
                        assert!(
                            work.permit().is_none(),
                            "reserved work cannot be spent through an alias"
                        );
                        batch.with((), |(), bound| {
                            // SAFETY: this FULL-item reservation settles exactly TWO items.
                            ((), unsafe { bound.take(TWO.get()) })
                        });
                        let work = active.turn().app();
                        assert_eq!(work.drain_count(), schedule::LIMIT - 2);
                    }
                }

                {
                    let active = turn.begin();
                    {
                        let observed = Reserved::reserve(active.turn().app(), NEXT, false);
                        assert!(
                            matches!(&observed, schedule::Capacity::Available(_)),
                            "complete budget"
                        );
                        assert!(active.turn().app().permit().is_none());
                    }
                    let work = active.turn().app();
                    assert_eq!(work.drain_count(), schedule::LIMIT);
                }

                {
                    let active = turn.begin();
                    let observed = Reserved::reserve(active.turn().app(), FULL, true);
                    assert!(
                        matches!(&observed, schedule::Capacity::Available(_)),
                        "preceding item and batch capacity"
                    );
                    if let schedule::Capacity::Available(batch) = observed {
                        assert_eq!(active.turn().app().drain_count(), 0);
                        batch.with((), |(), bound| {
                            // SAFETY: this FULL-item reservation settles exactly TWO items.
                            ((), unsafe { bound.take(TWO.get()) })
                        });
                        assert_eq!(active.turn().app().drain_count(), schedule::LIMIT - 2);
                    }
                }

                {
                    let active = turn.begin();
                    let observed = Reserved::reserve(active.turn().app(), FULL, true);
                    assert!(
                        matches!(&observed, schedule::Capacity::Available(_)),
                        "replayed batch capacity"
                    );
                    if let schedule::Capacity::Available(batch) = observed {
                        batch.with((), |(), bound| ((), bound.empty()));
                        assert_eq!(active.turn().app().drain_count(), schedule::LIMIT - 1);
                    }
                }

                {
                    let active = turn.begin();
                    let observed = Reserved::reserve(active.turn().app(), FOUR, false);
                    assert!(
                        matches!(&observed, schedule::Capacity::Available(_)),
                        "fresh batch capacity"
                    );
                    if let schedule::Capacity::Available(batch) = observed {
                        batch.with((), |(), bound| ((), bound.empty()));
                        assert_eq!(active.turn().app().drain_count(), schedule::LIMIT);
                    }
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn maint_share_is_nested() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            scope.with_driver(|_, mut turn| {
                let active = turn.begin();
                let work = active.turn().maint();

                work.share::<3, _>(|| {
                    assert_eq!(work.remaining(), schedule::LIMIT.div_ceil(3));
                    assert!(work.permit().is_some());

                    work.share::<2, _>(|| {
                        assert_eq!(
                            work.remaining(),
                            (schedule::LIMIT.div_ceil(3) - 1).div_ceil(2)
                        );
                        assert!(work.permit().is_some());
                    });
                    assert_eq!(work.remaining(), schedule::LIMIT.div_ceil(3) - 2);
                });
                assert_eq!(work.remaining(), schedule::LIMIT - 2);

                work.share::<{ usize::MAX }, _>(|| {
                    assert_eq!(work.remaining(), 1);
                    assert!(work.permit().is_some());
                });
                assert_eq!(work.remaining(), schedule::LIMIT - 3);

                work.share::<1, _>(|| {
                    assert_eq!(work.remaining(), schedule::LIMIT - 3);
                    assert!(work.permit().is_some());
                });
                assert_eq!(work.remaining(), schedule::LIMIT - 4);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn app_share_returns_unused() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            scope.with_driver(|_, mut turn| {
                let active = turn.begin();
                let work = active.turn().app();

                work.share::<3, _>(|share| {
                    assert_eq!(share.work().remaining(), schedule::LIMIT.div_ceil(3));
                    assert!(share.work().permit().is_some());
                });
                assert_eq!(work.remaining(), schedule::LIMIT - 1);

                work.share::<2, _>(|share| {
                    assert_eq!(share.work().remaining(), (schedule::LIMIT - 1).div_ceil(2));
                    assert!(share.work().permit().is_some());
                });
                assert_eq!(work.remaining(), schedule::LIMIT - 2);

                work.share::<1, _>(|share| {
                    assert_eq!(share.work().remaining(), schedule::LIMIT - 2);
                });
                assert_eq!(work.remaining(), schedule::LIMIT - 2);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
