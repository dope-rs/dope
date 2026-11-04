use crate::common::rt::{finish, recover};
use std::net::TcpStream;

use dope_core::{
    driver::{life::recovery, route},
    io::socket::{Listener, install::Install},
};
use dope_test::reclaim::Reports;

use crate::common::rt::Runtime;

pub mod common;

#[test]
fn adopts_known_listener() {
    let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener");
    let local = listener.local_addr();
    let runtime = Runtime::quic::<2, 8>();
    let mut reports = Reports::new::<route::Route>(runtime.config(), []);
    runtime.with_driver(
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
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
