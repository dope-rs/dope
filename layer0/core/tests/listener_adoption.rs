use std::{convert::Infallible, net::TcpStream};

use dope_core::{
    driver::life::recovery,
    io::socket::{Listener, install::Install},
};
use dope_test::scope::Error;

use crate::common::rt::Runtime;

pub mod common;

#[test]
fn adopts_known_listener() {
    let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener");
    let local = listener.local_addr();
    let mut reports = Vec::new();
    Runtime::quic::<2, 8>().with_driver(
        |mut driver| {
            let (listener, address) =
                Install::adopt_bound(driver.reborrow(), listener).expect("install listener");
            assert_eq!(address, local);
            let _peer = TcpStream::connect(address).expect("installed listener");
            listener.close(&mut driver);
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
