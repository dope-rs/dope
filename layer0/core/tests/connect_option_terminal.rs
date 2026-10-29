use std::{convert::Infallible, net::UdpSocket};

use dope_core::{
    driver::{
        life::recovery,
        retained::{self, raw::Connect},
        route::{FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind},
    },
    io::socket::{Inet, install::Install, option},
};
use dope_test::{reclaim::Reports, scope::Error};

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
                    let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                    let mut driver = retained::Context::new(driver, owner);
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
                        .try_cancel(driver.driver())
                        .map_err(|(_, error)| error)
                        .expect("completed cancellation");
                    let (socket, outcome) = pending.complete(done).ok().expect("matching connect");
                    assert!(matches!(outcome, Ok(())));
                    assert_eq!(source.peer_addr().expect("connected source"), peer);
                    socket.close(driver.driver());
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
