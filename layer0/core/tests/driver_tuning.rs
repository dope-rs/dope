pub mod common;

use std::{net::UdpSocket, os::fd::AsFd as _};

use dope_core::{
    Backend, Errno,
    dope_sys::io::socket::option as native_option,
    driver::{
        life::recovery,
        retained::flight,
        route::{FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind},
    },
    io::{
        fd::handles::Descriptor,
        socket::{install::Install, option, option::Tuning, pending},
    },
};
use dope_test::reclaim::Reports;

use crate::common::{
    driver::{Scoped as _, support::Targets as _, tuning::Tuned as _},
    rt::{Runtime, finish, recover},
};

#[test]
fn empty_tuning_preserves_nonempty_completion() {
    let setting = option::Buffer::try_from(4096_usize).expect("valid buffer size");
    let options = option::Set::new(Some(setting), Some(setting), None, None, None, None, None);
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let (socket, _) = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    )
                    .expect("bound datagram socket");
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let target = owner.operation::<kind::Tuning>();
                    let empty = Tuning::try_new(
                        &mut driver.reborrow(),
                        &flights,
                        owner,
                        socket.into(),
                        option::Set::EMPTY,
                    );
                    let Ok(Tuning::Applied(socket)) = empty else {
                        panic!("empty tuning must apply synchronously");
                    };
                    let tuning =
                        Tuning::try_new(&mut driver.reborrow(), &flights, owner, socket, options)
                            .map_err(|rejected| rejected.into_parts().1)
                            .expect("empty tuning leaves the flight slot available");
                    let socket = match tuning {
                        Tuning::Applied(socket) => socket,
                        Tuning::Pending(pending) => {
                            let done = driver.wait_tuning(turn).ok().expect("tuning completion");
                            assert!(target.matches(done.token()));
                            let (socket, outcome) = pending
                                .complete(done)
                                .ok()
                                .expect("completion matches its tuning authority");
                            assert_eq!(outcome, Ok(()));
                            socket
                        }
                    };
                    socket.close(&mut driver);
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
fn settled_tuning_reuses_flight() {
    let setting = option::Buffer::try_from(4096_usize).expect("valid buffer size");
    let options = option::Set::new(Some(setting), None, None, None, None, None, None);
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let (socket, _) = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    )
                    .expect("bound datagram socket");
                    let socket = Descriptor::from(socket);
                    let index = socket.index();
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let target = owner.operation::<kind::Tuning>();
                    let tuning =
                        Tuning::try_new(&mut driver.reborrow(), &flights, owner, socket, options)
                            .map_err(|rejected| rejected.into_parts().1)
                            .expect("first tuning");
                    let socket = match tuning {
                        Tuning::Applied(socket) => socket,
                        Tuning::Pending(pending) => {
                            let pending::Cancellation::Submitted(pending) =
                                pending.cancel(&mut driver)
                            else {
                                panic!("typed tuning cancellation");
                            };
                            let done = driver.wait_tuning(turn).ok().expect("tuning completion");
                            assert!(target.matches(done.token()));
                            let (socket, outcome) = pending
                                .complete(done)
                                .ok()
                                .expect("completion matches its cancellation authority");
                            assert!(outcome.is_ok() || outcome == Err(Errno::CANCELED));
                            socket
                        }
                    };
                    assert_eq!(socket.index(), index);
                    let tuning =
                        Tuning::try_new(&mut driver.reborrow(), &flights, owner, socket, options)
                            .map_err(|rejected| rejected.into_parts().1)
                            .expect("settled tuning releases the sole flight slot");
                    let socket = match tuning {
                        Tuning::Applied(socket) => socket,
                        Tuning::Pending(pending) => {
                            let done = driver.wait_tuning(turn).ok().expect("reused tuning");
                            assert!(target.matches(done.token()));
                            let (socket, outcome) = pending
                                .complete(done)
                                .ok()
                                .expect("completion matches its reused authority");
                            assert_eq!(outcome, Ok(()));
                            socket
                        }
                    };
                    assert_eq!(socket.index(), index);
                    socket.close(&mut driver);
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
fn failed_tuning_keeps_errno_and_reuses_descriptor() {
    let source = UdpSocket::bind(common::LOCALHOST).expect("source socket");
    let expected = <Backend as native_option::Source>::set(
        source.as_fd(),
        native_option::Kind::NoDelay,
        &native_option::Value::from_enabled(true),
    )
    .expect_err("TCP option on a UDP socket");
    let setting = option::Buffer::try_from(4096_usize).expect("valid buffer size");
    let options = option::Set::new(Some(setting), None, None, None, None, None, None);
    let invalid = option::Set::new(None, None, Some(option::Switch::On), None, None, None, None);
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let next = driver
                        .storage
                        .reborrow()
                        .try_flights(flight::Need::<KeyTag<2>>::new(
                            flight::Capacity::fixed::<1>(),
                        ))
                        .expect("next route flight");
                    let next = next.slots();
                    let (socket, _) = Install::adopt_udp(driver.reborrow(), source.into())
                        .expect("adopt source socket");
                    let socket = Descriptor::from(socket);
                    let index = socket.index();
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let target = owner.operation::<kind::Tuning>();
                    let (socket, error) = match Tuning::try_new(
                        &mut driver.reborrow(),
                        &flights,
                        owner,
                        socket,
                        invalid,
                    ) {
                        Err(rejected) => {
                            let (socket, cause) = rejected.into_parts();
                            let pending::Cause::Option(error) = cause else {
                                panic!("invalid option must preserve its native error");
                            };
                            (socket, error)
                        }
                        Ok(Tuning::Pending(pending)) => {
                            let done = driver.wait_tuning(turn).ok().expect("failed tuning");
                            assert!(target.matches(done.token()));
                            let (socket, outcome) = pending
                                .complete(done)
                                .ok()
                                .expect("failure completion matches its tuning authority");
                            (socket, outcome.expect_err("TCP option on a UDP socket"))
                        }
                        Ok(Tuning::Applied(_)) => panic!("invalid option was accepted"),
                    };
                    assert_eq!(error, expected);
                    assert_eq!(socket.index(), index);
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<2>>(SlotIndex::ZERO);
                    let target = owner.operation::<kind::Tuning>();
                    let tuning =
                        Tuning::try_new(&mut driver.reborrow(), &next, owner, socket, options)
                            .map_err(|rejected| rejected.into_parts().1)
                            .expect("failed tuning releases its transaction");
                    let socket = match tuning {
                        Tuning::Applied(socket) => socket,
                        Tuning::Pending(pending) => {
                            let done = driver.wait_tuning(turn).ok().expect("next route tuning");
                            assert!(target.matches(done.token()));
                            let (socket, outcome) = pending
                                .complete(done)
                                .ok()
                                .expect("completion matches its next route authority");
                            assert_eq!(outcome, Ok(()));
                            socket
                        }
                    };
                    assert_eq!(socket.index(), index);
                    socket.close(&mut driver);
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
