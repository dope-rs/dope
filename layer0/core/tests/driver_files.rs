use std::convert::Infallible;

use dope_core::{
    driver,
    driver::{life::recovery, route::KeyTag},
    io::{
        fd::{handles::Descriptor, outbound::Reserved},
        socket::{Open, install::Install},
    },
};
use dope_test::scope::Error;

use crate::common::driver::{Native as _, Scoped as _};

pub mod common;

use std::{env::consts, ops::ControlFlow, time::Duration};

use dope_core::{
    driver::{
        result::poll,
        route::{
            App as _, Epoch, FrameworkTag, Tag, Target, indices::SlotIndex, kind, table::Capacity,
        },
        schedule::state::scope::Guard,
        settings,
    },
    io::{
        event::{
            Kind,
            net::socket::{Completion, Failure, Finished, Outcome},
        },
        socket,
        socket::{install, install::slot},
    },
};
use dope_test::reclaim::Reports;
use socket::Prepared as _;

use crate::common::{driver::Wait, rt::Runtime};

type Route = KeyTag<7>;
const ONE: Capacity = settings::FileSlots::fixed::<0, 1>().outbound();
const TWO: Capacity = settings::FileSlots::fixed::<0, 2>().outbound();
const SIXTEEN: Capacity = settings::FileSlots::fixed::<0, 16>().outbound();

fn wait_socket<'d>(
    driver: &mut driver::Context<'_, 'd>,
    turn: &mut Guard<'_, 'd>,
    owner: Target<'d, Route>,
) -> Option<Completion<'d>> {
    assert!(
        driver::Context::wait(driver, turn.reactor(), Some(Duration::from_secs(1))).is_ok(),
        "socket creation wait"
    );
    let mut event = None;
    assert!(matches!(
        driver::Context::dispatch(driver, turn.reactor(), |completion, _source| {
            let done = match completion.into_kind() {
                Kind::Socket(done) => done,
                event => return ControlFlow::Break(event.into()),
            };
            assert!(
                owner.operation::<kind::Socket>().matches(done.token()),
                "socket completion retains its routed owner"
            );
            assert!(event.replace(done).is_none());
            ControlFlow::Continue(())
        }),
        poll::Dispatch::Ready { retained: None, .. }
    ));
    event
}

fn reclaim<'d>(driver: &mut driver::Context<'_, 'd>, turn: &mut Guard<'_, 'd>) {
    let address = ([127, 0, 0, 1], 0).into();
    let error = (Install::bind_udp(driver.reborrow(), address, common::DATAGRAM))
        .err()
        .expect("retiring slots remain pinned until completion");
    assert!(matches!(
        error,
        install::Error::Slot(slot::Error::Retiring { available: 0 })
    ));

    assert!(
        matches!(
            driver::Context::commit(driver, turn.reactor()),
            Ok(poll::Commit::Drained)
        ),
        "submit close"
    );
    let result = driver.rebind(turn);
    assert!(
        result.is_ok(),
        "close completion releases the retired slots"
    );
    if let Ok((socket, _)) = result {
        Descriptor::from(socket).close(driver);
    }
}

#[test]
fn rejects_capacity_early() {
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<0, 1>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            let mut driver = scope.context();
            let config = socket::Config::default();
            let address = ([127, 0, 0, 1], 0).into();
            let result = Install::bind_stream(
                driver.reborrow(),
                address,
                socket::Backlog::fixed::<1>(),
                &config,
            );
            let (listener, bound) = result.expect("bound listener");
            let error = (Install::bind_stream(
                driver.reborrow(),
                bound,
                socket::Backlog::fixed::<1>(),
                &config,
            ))
            .err()
            .expect("descriptor admission must precede socket creation");
            assert!(matches!(
                error,
                install::Error::Slot(slot::Error::Capacity { available: 0 })
            ));
            listener.close(&mut driver);
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
fn failed_bind_reuses_slots() {
    let held =
        std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("occupied address");
    let address = held.local_addr().expect("bound address");
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<0, 1>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    runtime.with_scope(
        |scope| {
            let mut driver = scope.context();
            for _ in 0..2 {
                assert!(matches!(
                    Install::bind_stream(
                        driver.reborrow(),
                        address,
                        socket::Backlog::fixed::<1>(),
                        &socket::Config::default(),
                    ),
                    Err(install::Error::System {
                        operation: install::Operation::Bind,
                        fault,
                    }) if fault.code().get() == libc::EADDRINUSE
                ));
            }
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
fn rejects_foreign_creator() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports = Reports::<_>::new(
        runtime.config(),
        [<FrameworkTag<kind::Any> as Tag>::ROUTE, Route::ID.into()],
    );

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<Route, 2>(|mut driver, turn, flights| {
                    let stream = socket::Inet::from_std(([127, 0, 0, 1], 9).into()).stream_spec();
                    let second = {
                        let slots = (Reserved::<Route>::try_new(&driver, TWO))
                            .expect("reserve two outbound slots");
                        let own1 = driver
                            .driver_ref()
                            .targets::<Route>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL);
                        let first = (slots.bind(own1)).expect("bind first slot");
                        let _first = Open::try_new(&mut driver, &flights, first, stream)
                            .map(|open| open.commit())
                            .expect("submit first socket creation");
                        let done1 =
                            (wait_socket(&mut driver, turn, own1)).expect("socket completion");

                        let own2 = driver
                            .driver_ref()
                            .targets::<Route>()
                            .bind(SlotIndex::from(1_u16), Epoch::INITIAL);
                        let second = (slots.bind(own2)).expect("bind second slot");
                        let open = (Open::try_new(&mut driver, &flights, second, stream))
                            .expect("submit second socket creation");
                        let second = open.commit();
                        let done2 =
                            (wait_socket(&mut driver, turn, own2)).expect("socket completion");

                        let mismatch = (done1.try_finish(second))
                            .err()
                            .expect("different physical creation authorities must not activate");
                        let second = mismatch.reclaim();
                        done2.try_finish(second)
                    };
                    assert!(
                        matches!(&second, Ok(Finished::Created(_))),
                        "second socket creation must succeed"
                    );
                    if let Ok(Finished::Created(second)) = second {
                        second.close(&mut driver);
                    }
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
fn drop_waits_for_result() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<15, 1>());
    let mut reports = Reports::<_>::new(
        runtime.config(),
        [<FrameworkTag<kind::Any> as Tag>::ROUTE, Route::ID.into()],
    );

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<Route, 1>(|mut driver, turn, flights| {
                    let stream = socket::Inet::from_std(([127, 0, 0, 1], 9).into()).stream_spec();
                    {
                        let slots = (Reserved::<Route>::try_new(&driver, ONE))
                            .expect("reserve the outbound slot");
                        let owner = driver
                            .driver_ref()
                            .targets::<Route>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL);
                        {
                            let request = (slots.bind(owner)).expect("bind outbound slot");
                            let _create = Open::try_new(&mut driver, &flights, request, stream)
                                .map(|open| open.commit())
                                .expect("submit socket creation");

                            assert!(slots.bind(owner).is_none());
                        }
                        let done =
                            (wait_socket(&mut driver, turn, owner)).expect("socket completion");
                        let outcome = done.into_parts().1;
                        assert!(
                            matches!(outcome, Outcome::Failed(Failure::Retired)),
                            "released creation authority must retire its completion"
                        );
                    }

                    assert!(
                        matches!(
                            driver::Context::commit(&mut driver, turn.reactor()),
                            Ok(poll::Commit::Drained)
                        ),
                        "submit close"
                    );
                    for _ in 0..2 {
                        let result = Wait {
                            driver: &mut driver,
                            turn,
                            timeout: Duration::from_secs(1),
                        }
                        .try_run();
                        result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("operation completion");
                    }
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
fn late_owner_joins_close() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports = Reports::<_>::new(
        runtime.config(),
        [<FrameworkTag<kind::Any> as Tag>::ROUTE, Route::ID.into()],
    );

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<Route, 1>(|mut driver, turn, flights| {
                    let stream = socket::Inet::from_std(([127, 0, 0, 1], 9).into()).stream_spec();
                    {
                        let slots = (Reserved::<Route>::try_new(&driver, SIXTEEN))
                            .expect("reserve every fixed slot");
                        let owner = driver
                            .driver_ref()
                            .targets::<Route>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL);
                        let request = (slots.bind(owner)).expect("bind outbound slot");
                        let _create = Open::try_new(&mut driver, &flights, request, stream)
                            .map(|open| open.commit())
                            .expect("submit socket creation");
                        let done =
                            (wait_socket(&mut driver, turn, owner)).expect("socket completion");
                        let outcome = done.into_parts().1;
                        assert!(
                            matches!(&outcome, Outcome::Created(_)),
                            "socket creation must succeed"
                        );
                        if let Outcome::Created(created) = outcome {
                            drop(created);
                        }
                    }

                    reclaim(&mut driver, turn);
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
fn stale_creator_pins_slot() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports = Reports::<_>::new(
        runtime.config(),
        [<FrameworkTag<kind::Any> as Tag>::ROUTE, Route::ID.into()],
    );

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<Route, 1>(|mut driver, turn, flights| {
                    let stream = socket::Inet::from_std(([127, 0, 0, 1], 9).into()).stream_spec();
                    let slots =
                        (Reserved::<Route>::try_new(&driver, ONE)).expect("reserve one fixed slot");
                    let owner = driver
                        .driver_ref()
                        .targets::<Route>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL);
                    {
                        let created = {
                            let request = (slots.bind(owner)).expect("bind outbound slot");
                            let _create = Open::try_new(&mut driver, &flights, request, stream)
                                .map(|open| open.commit())
                                .expect("submit socket creation");
                            let done =
                                (wait_socket(&mut driver, turn, owner)).expect("socket completion");
                            done.into_parts().1
                        };
                        assert!(
                            matches!(&created, Outcome::Created(_)),
                            "socket creation must succeed"
                        );
                        if let Outcome::Created(created) = created {
                            assert!(
                                matches!(
                                    driver::Context::commit(&mut driver, turn.reactor()),
                                    Ok(poll::Commit::Drained)
                                ),
                                "submit close"
                            );
                            for _ in 0..2 {
                                let result = Wait {
                                    driver: &mut driver,
                                    turn,
                                    timeout: Duration::from_secs(1),
                                }
                                .try_run();
                                result
                                    .as_ref()
                                    .map_err(|error| error.cause())
                                    .expect("operation completion");
                            }
                            assert!(
                                slots.bind(owner).is_none(),
                                "a stale creation proof must pin its exact slot"
                            );
                            drop(created);
                        }
                    }

                    assert!(
                        matches!(
                            driver::Context::commit(&mut driver, turn.reactor()),
                            Ok(poll::Commit::Drained)
                        ),
                        "release stale creation proof"
                    );
                    let rebound = slots.bind(owner);
                    assert!(
                        rebound.is_some(),
                        "releasing both affine halves must release the slot"
                    );
                    assert!(slots.bind(owner).is_none());
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
fn retiring_waits_close() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports = Reports::<_>::new(
        runtime.config(),
        [<FrameworkTag<kind::Any> as Tag>::ROUTE, Route::ID.into()],
    );

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<Route, 1>(|mut driver, turn, flights| {
                    let stream = socket::Inet::from_std(([127, 0, 0, 1], 9).into()).stream_spec();
                    let fd = {
                        let slots = (Reserved::<Route>::try_new(&driver, SIXTEEN))
                            .expect("reserve every fixed slot");
                        let owner = driver
                            .driver_ref()
                            .targets::<Route>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL);
                        let request = (slots.bind(owner)).expect("bind outbound slot");
                        let open = (Open::try_new(&mut driver, &flights, request, stream))
                            .expect("submit socket creation");
                        let creating = open.commit();
                        let done =
                            (wait_socket(&mut driver, turn, owner)).expect("socket completion");
                        done.try_finish(creating)
                    };
                    assert!(
                        matches!(&fd, Ok(Finished::Created(_))),
                        "socket creation must finish with its creating authority"
                    );
                    if let Ok(Finished::Created(fd)) = fd {
                        fd.close(&mut driver);
                        reclaim(&mut driver, turn);
                    }
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
