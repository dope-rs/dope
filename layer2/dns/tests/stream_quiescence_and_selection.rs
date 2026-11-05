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

#[test]
fn tcp_quiet_selection() {
    let server = stream::Server::new().expect("stream fixture");
    let servers = config::Servers::try_new([server.other, server.address]).expect("server list");
    let mut received = Vec::new();
    let prepared = fixture::try_prepare::<OUTBOUND, LANES, ADDRESSES>(
        fixture::configure(servers, 2),
        &mut received,
    )
    .expect("DNS resources");
    let mut finish = Completion::new();
    let tail = stream::Tail {
        finish: &mut finish,
        server,
    };
    let mut outcome = None;
    prepared.try_scoped(
        |scope| {
            scope.lend(|install, stage| stream::compose(tail, install, stage, &mut outcome));
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
    );
    assert!(
        received.is_empty(),
        "unexpected datagram failure: {received:?}"
    );
    outcome
        .expect("DNS scope ran")
        .expect("quiet stream lookup");
}
