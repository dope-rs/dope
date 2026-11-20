pub mod common;

use std::{cell::Cell, net::UdpSocket};

use common::{
    peer, recovery,
    retry::{BIND, ENDPOINT, Events, domains, traces::Immediate},
    suite::peers::tls,
};
use dope::{
    core::driver::route::Id,
    runtime::{
        executor::session,
        shutdown,
    },
};
use dope_quic::{
    client::{self, build},
    runtime::endpoint::client::Plan,
};
use dope_test::harness::scenario::TIMEOUT;

#[test]
fn rejects_another_dialer_attempt() {
    let (_, public) = peer::raw(0x4d).expect("raw identity");
    let receiver = UdpSocket::bind(BIND).expect("quiet peer");
    let address = receiver.local_addr().expect("peer address");
    let first = Plan::<1>::new(BIND, ENDPOINT).expect("first endpoint");
    let second = Plan::<2>::new(BIND, ENDPOINT).expect("second endpoint");
    let attempts = [Cell::new(None), Cell::new(None)];
    let checked = Cell::new(false);
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let executor = recovery::executor(
        source,
        Events::settings(16, (first.as_ref(), second.as_ref())).expect("driver claims"),
    );
    let ((), reports) = recovery::run(executor, |mut session| {
        let (first, second, timeout) = {
            let mut driver = session.driver();
            let timeout = driver
                .driver_ref()
                .clock()
                .now()
                .try_add(domains::TIMEOUT)
                .expect("test deadline");
            let config = client::Config {
                event_budget: 1,
                retry_budget: 1,
            };
            let first = build::Builder::owned_with(
                vec![client::EndpointSpec {
                    addr: address,
                    authority: tls::client(public).expect("first authority"),
                }],
                domains::Config(&attempts[0]),
                domains::protocol(),
                Immediate::default(),
                config,
            )
            .bind(first, &mut driver)
            .expect("first dialer");
            let second = build::Builder::owned_with(
                vec![client::EndpointSpec {
                    addr: address,
                    authority: tls::client(public).expect("second authority"),
                }],
                domains::Config(&attempts[1]),
                domains::protocol(),
                Immediate::default(),
                config,
            )
            .bind(second, &mut driver)
            .expect("second dialer");
            (first, second, timeout)
        };
        let mut report = session::Completion::new();
        session
            .with_quiesce(
                &mut report,
                domains::App {
                    first,
                    second,
                    attempts: &attempts,
                    checked: &checked,
                    stop: Some(stop),
                    timeout,
                },
                TIMEOUT,
                |app| app.run_for(TIMEOUT).map(|_| ()),
            )
            .expect("session completion")
            .expect("application completion");
    });
    assert!(
        checked.get(),
        "both dialers must retain their first live attempt"
    );
    recovery::Teardown::new([Id::fixed::<1>(), Id::fixed::<2>()])
        .verify(reports)
        .expect("exact receive retirement");
}
