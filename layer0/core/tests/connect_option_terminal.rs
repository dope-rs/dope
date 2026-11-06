use crate::common::rt::{finish, recover};
use std::net::UdpSocket;

use dope_core::{
    driver::{
        life::recovery,
        retained::raw::Connect,
        route::{FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind},
    },
    io::socket::{Inet, install::Install, option},
};
use dope_test::reclaim::Reports;

use crate::common::{
    driver::{Scoped as _, support::Targets as _, tuning::Tuned as _},
    rt::Runtime,
};

pub mod common;

#[test]
fn completed_connect_cancel() {
    let source = UdpSocket::bind(common::LOCALHOST).expect("source");
    let dest = UdpSocket::bind(common::LOCALHOST).expect("dest");
    let peer = dest.local_addr().expect("dest address");
    let address = Inet::from_std(peer);
    let buffer = option::Buffer::try_from(4096_usize).expect("buffer size");
    let options = option::Set::new(Some(buffer), Some(buffer), None, None, None, None, None);
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let (socket, _) = Install::adopt_udp(
                        driver.reborrow(),
                        source.try_clone().expect("observable source").into(),
                    )
                    .expect("source descriptor");
                    let target = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    // SAFETY: address remains fixed outside the driver scope,
                    // through completion and final native quiescence.
                    let pending = unsafe {
                        Connect::new(socket.into(), &address, target).try_connect(
                            driver.reborrow(),
                            &flights,
                            &options,
                        )
                    }
                    .map_err(|rejected| rejected.into_parts().1)
                    .expect("connect submission");
                    let done = driver.wait_connect(turn).ok().expect("connect completion");
                    let pending = pending
                        .try_cancel(&mut driver)
                        .map_err(|(_, error)| error)
                        .expect("completed cancellation");
                    let (socket, outcome) = pending.complete(done).ok().expect("matching connect");
                    assert!(matches!(outcome, Ok(())));
                    assert_eq!(source.peer_addr().expect("connected source"), peer);
                    socket.close(&mut driver);
                })
                .expect("connect flight");
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
fn connect_completion_matches_native_flight() {
    let sources = [
        UdpSocket::bind(common::LOCALHOST).expect("first source"),
        UdpSocket::bind(common::LOCALHOST).expect("second source"),
    ];
    let dest = UdpSocket::bind(common::LOCALHOST).expect("dest");
    let address = Inet::from_std(dest.local_addr().expect("dest address"));
    let options = option::Set::default();
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let mut rejected = false;
    let mut completed = false;

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 2>(|mut driver, turn, flights| {
                    let target = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let [(first, first_done), (second, second_done)] = sources.map(|source| {
                        let (socket, _) = Install::adopt_udp(driver.reborrow(), source.into())
                            .expect("source descriptor");
                        // SAFETY: address remains fixed outside the driver scope,
                        // through both completions and final native quiescence.
                        let pending = unsafe {
                            Connect::new(socket.into(), &address, target).try_connect(
                                driver.reborrow(),
                                &flights,
                                &options,
                            )
                        }
                        .map_err(|rejected| rejected.into_parts().1)
                        .expect("connect submission");
                        let done = driver.wait_connect(turn).ok().expect("connect completion");
                        (pending, done)
                    });
                    // Both native operations are terminal before testing the mismatch,
                    // so even the defective acceptance path can settle safely.
                    let (first, second) = match second.complete(first_done) {
                        Err(mismatch) => {
                            rejected = true;
                            let (second, first_done) = mismatch.into_parts();
                            (
                                first.complete(first_done).ok().expect("first connect"),
                                second.complete(second_done).ok().expect("second connect"),
                            )
                        }
                        Ok(second) => (
                            first.complete(second_done).ok().expect("crossed connect"),
                            second,
                        ),
                    };
                    completed = first.1.is_ok() && second.1.is_ok();
                    first.0.close(&mut driver);
                    second.0.close(&mut driver);
                })
                .expect("connect flights");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert!(completed, "both connects must complete successfully");
    assert!(
        rejected,
        "same-target completion from another native flight was accepted"
    );
}
