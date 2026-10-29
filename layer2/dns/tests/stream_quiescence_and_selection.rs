use std::convert::Infallible;
pub mod common;

use common::{
    fixture,
    refresh::{ADDRESSES, LANES, OUTBOUND},
    stream,
};
use dope::{
    core::driver::life::recovery::{Policy, Recovery},
    runtime::executor::session::Completion,
};
use dope_dns::config;
use dope_test::harness::scenario::run::Task as _;

#[test]
fn tcp_quiet_selection() {
    let (server, signals) = stream::Server::new().expect("stream fixture");
    let servers = config::Servers::try_new([server.other, server.address]).expect("server list");
    let mut received = Vec::new();
    let prepared = fixture::try_prepare::<OUTBOUND, LANES, ADDRESSES>(
        fixture::configure(servers, 2),
        &mut received,
    )
    .expect("DNS resources");
    let mut finish = Completion::new();
    let tail = signals.tail(&mut finish).expect("stream readiness");
    let mut outcome = None;
    let ((), worker) = server
        .with(|| {
            prepared.try_scoped(
                |scope| {
                    scope
                        .lend(|install, stage| stream::compose(tail, install, stage, &mut outcome));
                },
                Policy::new(
                    |report| fixture::reclaim(report).expect("DNS reclaim"),
                    |failure: Recovery<'_, '_>| -> Infallible {
                        panic!("DNS recovery: {:?}", failure.cause())
                    },
                ),
                |result| {
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("DNS scope");
                },
            )
        })
        .expect("stream worker start");
    assert!(
        received.is_empty(),
        "unexpected datagram failure: {received:?}"
    );
    worker
        .expect("DNS worker joined")
        .expect("stream fixture completed");
    outcome
        .expect("DNS scope ran")
        .expect("install DNS")
        .expect("quiet stream lookup");
}
