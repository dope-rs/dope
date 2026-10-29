use std::convert::Infallible;

use dope_core::{
    driver,
    driver::life::recovery,
    io::socket::{install::Install, option::Tuning},
};
use dope_test::scope::Error;

use crate::common::driver::{Scoped as _, support::Targets as _, tuning::Tuned as _};

pub mod common;
use std::env::consts;

use dope_core::{
    driver::{
        retained::flight,
        route::{FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind},
        schedule::state::scope,
    },
    io::socket::{option, pending},
};
use dope_test::reclaim::Reports;

use crate::common::rt::Runtime;

fn reuse<'d, const NEXT: u8>(
    mut driver: driver::Context<'_, 'd>,
    turn: &mut scope::Guard<'_, 'd>,
    slots1: &flight::Slots<'d, KeyTag<1>>,
    slots2: &flight::Slots<'d, KeyTag<NEXT>>,
) {
    let setting = option::Buffer::try_from(4096_usize);
    let setting = setting.expect("valid buffer size");
    let options = option::Set::new(Some(setting), None, None, None, None, None, None);

    let socket = Install::bind_udp(
        driver.reborrow(),
        ([127, 0, 0, 1], 0).into(),
        common::DATAGRAM,
    );
    let (socket, _) = socket.expect("bound datagram socket");
    let owner1 = driver
        .driver_ref()
        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
    let first = owner1.operation::<kind::Tuning>();
    let observed = Tuning::try_new(
        &mut driver.reborrow(),
        slots1,
        owner1,
        socket.into(),
        options,
    );
    assert!(
        matches!(&observed, Ok(option::Tuning::Pending(_))),
        "io_uring tuning must be asynchronous"
    );
    if let Ok(option::Tuning::Pending(wait1)) = observed {
        let done = (driver.wait_tuning(turn)).ok().expect("tuning completion");
        assert!(first.matches(done.token()));
        let complete = wait1.complete(done);
        assert!(
            complete.is_ok(),
            "first completion must match its authority"
        );
        let (socket, _) = complete.ok().expect("matching completion owns the socket");
        let owner2 = driver
            .driver_ref()
            .tuned_target::<KeyTag<NEXT>>(SlotIndex::ZERO);
        let second = owner2.operation::<kind::Tuning>();
        let observed = Tuning::try_new(&mut driver.reborrow(), slots2, owner2, socket, options);
        assert!(
            matches!(&observed, Ok(option::Tuning::Pending(_))),
            "io_uring tuning must be asynchronous"
        );
        if let Ok(option::Tuning::Pending(wait2)) = observed {
            let done = (driver.wait_tuning(turn)).ok().expect("tuning completion");
            assert!(second.matches(done.token()));
            let complete = wait2.complete(done);
            assert!(
                complete.is_ok(),
                "reused completion must match its new authority"
            );
            let (socket, _) = complete.ok().expect("matching completion owns the socket");
            socket.close(&mut driver);
        }
    }
}

#[test]
fn empty_tuning_frees_slot() {
    let setting = option::Buffer::try_from(4096_usize);
    let setting = setting.expect("valid buffer size");
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let options =
                        option::Set::new(Some(setting), None, None, None, None, None, None);

                    let socket = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    );
                    let (socket, _) = socket.expect("bound datagram socket");
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let observed = Tuning::try_new(
                        &mut driver.reborrow(),
                        &flights,
                        owner,
                        socket.into(),
                        option::Set::EMPTY,
                    );
                    assert!(
                        matches!(&observed, Ok(option::Tuning::Applied(_))),
                        "empty tuning must apply synchronously"
                    );
                    if let Ok(option::Tuning::Applied(socket)) = observed {
                        let observed = Tuning::try_new(
                            &mut driver.reborrow(),
                            &flights,
                            owner,
                            socket,
                            options,
                        );
                        assert!(
                            matches!(&observed, Ok(option::Tuning::Pending(_))),
                            "empty tuning must leave the fixed slot vacant"
                        );
                        if let Ok(option::Tuning::Pending(pending)) = observed {
                            let done = (driver.wait_tuning(turn)).ok().expect("tuning completion");
                            let complete = pending.complete(done);
                            assert!(
                                complete.is_ok(),
                                "completion must match its tuning authority"
                            );
                            let (socket, _) =
                                complete.ok().expect("matching completion owns the socket");
                            socket.close(&mut driver);
                        }
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
fn tuning_event_is_public() {
    let setting = option::Buffer::try_from(4096_usize);
    let setting = setting.expect("valid buffer size");
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let options = option::Set::new(
                        Some(setting),
                        Some(setting),
                        None,
                        None,
                        None,
                        None,
                        None,
                    );

                    let socket = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    );
                    let (socket, _) = socket.expect("bound datagram socket");

                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let target = owner.operation::<kind::Tuning>();
                    let observed = Tuning::try_new(
                        &mut driver.reborrow(),
                        &flights,
                        owner,
                        socket.into(),
                        options,
                    );
                    assert!(
                        matches!(&observed, Ok(option::Tuning::Pending(_))),
                        "io_uring tuning must be asynchronous"
                    );
                    if let Ok(option::Tuning::Pending(pending)) = observed {
                        let done = (driver.wait_tuning(turn)).ok().expect("tuning completion");
                        assert!(target.matches(done.token()));
                        let complete = pending.complete(done);
                        assert!(
                            complete.is_ok(),
                            "completion must match its tuning authority"
                        );
                        let (socket, event) =
                            complete.ok().expect("matching completion owns the socket");
                        assert!(matches!(event, Ok(())));
                        socket.close(&mut driver);
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
fn cancel_keeps_slot() {
    let setting = option::Buffer::try_from(4096_usize);
    let setting = setting.expect("valid buffer size");
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let options =
                        option::Set::new(Some(setting), None, None, None, None, None, None);

                    let socket = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    );
                    let (socket, _) = socket.expect("bound datagram socket");
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let target = owner.operation::<kind::Tuning>();
                    let observed = Tuning::try_new(
                        &mut driver.reborrow(),
                        &flights,
                        owner,
                        socket.into(),
                        options,
                    );
                    assert!(
                        matches!(&observed, Ok(option::Tuning::Pending(_))),
                        "io_uring tuning must be asynchronous"
                    );
                    if let Ok(option::Tuning::Pending(pending)) = observed {
                        let outcome = pending.cancel(&mut driver);
                        let observed = outcome;
                        assert!(
                            matches!(&observed, pending::Cancellation::Submitted(_)),
                            "typed tuning cancellation"
                        );
                        if let pending::Cancellation::Submitted(pending) = observed {
                            let done = (driver.wait_tuning(turn)).ok().expect("tuning completion");
                            assert!(target.matches(done.token()));
                            let complete = pending.complete(done);
                            assert!(
                                complete.is_ok(),
                                "completion must match its cancellation authority"
                            );
                            let (socket, _) =
                                complete.ok().expect("matching completion owns the socket");
                            let observed = Tuning::try_new(
                                &mut driver.reborrow(),
                                &flights,
                                owner,
                                socket,
                                options,
                            );
                            assert!(
                                matches!(&observed, Ok(option::Tuning::Pending(_))),
                                "io_uring tuning must be asynchronous"
                            );
                            if let Ok(option::Tuning::Pending(reused)) = observed {
                                let done =
                                    (driver.wait_tuning(turn)).ok().expect("tuning completion");
                                let complete = reused.complete(done);
                                assert!(
                                    complete.is_ok(),
                                    "completion must match the reused authority"
                                );
                                let (socket, _) =
                                    complete.ok().expect("matching completion owns the socket");
                                socket.close(&mut driver);
                            }
                        }
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
fn completed_tuning_reuses() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, slots1| {
                    let slots2 =
                        (driver
                            .storage
                            .reborrow()
                            .try_flights(flight::Need::<KeyTag<2>>::new(
                                flight::Capacity::fixed::<1>(),
                            )))
                        .expect("tuning flights");
                    let slots2 = slots2.slots();
                    reuse(driver, turn, &slots1, &slots2);
                })
                .expect("flight fixture");
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
fn tuning_keeps_errno() {
    let setting = option::Buffer::try_from(4096_usize);
    let setting = setting.expect("valid buffer size");
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, slots1| {
                    let options =
                        option::Set::new(Some(setting), None, None, None, None, None, None);
                    let invalid = option::Set::new(
                        Some(setting),
                        Some(setting),
                        Some(option::Switch::On),
                        None,
                        None,
                        None,
                        None,
                    );

                    let slots2 =
                        (driver
                            .storage
                            .reborrow()
                            .try_flights(flight::Need::<KeyTag<2>>::new(
                                flight::Capacity::fixed::<1>(),
                            )))
                        .expect("tuning flights");
                    let slots2 = slots2.slots();
                    let socket = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    );
                    let (socket, _) = socket.expect("bound datagram socket");
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let observed = Tuning::try_new(
                        &mut driver.reborrow(),
                        &slots1,
                        owner,
                        socket.into(),
                        invalid,
                    );
                    assert!(
                        matches!(&observed, Ok(option::Tuning::Pending(_))),
                        "io_uring tuning must be asynchronous"
                    );
                    if let Ok(option::Tuning::Pending(pending)) = observed {
                        let done = (driver.wait_tuning(turn)).ok().expect("tuning completion");
                        let complete = pending.complete(done);
                        assert!(
                            complete.is_ok(),
                            "failure completion must match its authority"
                        );
                        let (socket, event) =
                            complete.ok().expect("matching completion owns the socket");
                        assert!(matches!(
                            event,
                            Err(error) if error.code().get() == libc::ENOPROTOOPT
                        ));
                        let owner = driver
                            .driver_ref()
                            .tuned_target::<KeyTag<2>>(SlotIndex::ZERO);
                        let observed = Tuning::try_new(
                            &mut driver.reborrow(),
                            &slots2,
                            owner,
                            socket,
                            options,
                        );
                        assert!(
                            matches!(&observed, Ok(option::Tuning::Pending(_))),
                            "failed tuning must release its fixed-slot transaction"
                        );
                        if let Ok(option::Tuning::Pending(reused)) = observed {
                            let done = (driver.wait_tuning(turn)).ok().expect("tuning completion");
                            let complete = reused.complete(done);
                            assert!(
                                complete.is_ok(),
                                "reused completion must match its tuning authority"
                            );
                            let (socket, event) =
                                complete.ok().expect("matching completion owns the socket");
                            assert!(matches!(event, Ok(())));
                            socket.close(&mut driver);
                        }
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
fn reuses_descriptor() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|driver, turn, flights| {
                    reuse(driver, turn, &flights, &flights);
                })
                .expect("flight fixture");
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
