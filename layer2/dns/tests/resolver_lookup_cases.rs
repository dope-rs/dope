use std::convert::Infallible;
pub mod common;

use std::{
    iter,
    mem::size_of,
    net::SocketAddr,
    time::{Duration, Instant},
};

use dope::{
    core::{
        Errno,
        driver::{
            life::recovery::{Policy, Recovery},
            settings,
        },
        io::datagram::ReceiveError,
    },
    manifold::client::close,
    runtime::{executor::session::Completion, topology::lending},
};
use dope_dns::{
    config, host,
    lookup::error::{Error, Kind, Malformed, Packet},
};
use dope_test::{alloc::Tracker, harness::scenario::run::Task as _};

use crate::common::{
    fixture,
    lookup::{
        self, Query,
        cases::*,
        failure::App,
        server::{Cookie, Mode, Peer, Reject, Reply},
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn reclaims_live_lane() {
    let server = Peer::new(FILTER, 2).expect("two-lane peer");
    let servers = config::Servers::try_new([server.addr]).expect("configured DNS peer");
    let ((result, received, _), worker) = server
        .with(|| resolve_configured::<2>("api.example:443", fixture::configure(servers, 1)))
        .expect("two-lane worker start");
    assert_eq!(
        worker
            .expect("DNS worker joined")
            .expect("live lane response"),
        2
    );
    assert!(received.is_empty());
    check(result, Ok(1));
}

#[test]
fn reports_receive_errors() {
    use dope::core::io::datagram::{Cause, ReceiveError};

    let server = Peer::new(Mode::Oversized, 2).expect("oversized datagram peer");
    let servers = config::Servers::try_new([server.addr]).expect("configured DNS peer");
    let ((result, received, receive), worker) = server
        .with(|| {
            resolve_observed(
                "api.example:443",
                servers,
                1,
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
        })
        .expect("DNS worker start");
    assert_eq!(
        worker
            .expect("DNS worker joined")
            .expect("oversized peer finished"),
        2
    );
    check(result, Ok(11));
    assert_eq!(
        received.len(),
        2,
        "each malformed receive must be observed: {received:?}"
    );
    let capacity = receive.buffer_len().into_nonzero().get() as usize
        - 4 * size_of::<u32>()
        - size_of::<libc::sockaddr_storage>();
    let expected = [65_507, capacity].map(|payload| {
        ReceiveError::Truncated(Cause::Flags {
            actual: libc::MSG_TRUNC as u32,
            payload,
            stored: capacity,
        })
    });
    for cause in received {
        assert!(
            expected.contains(&cause),
            "unexpected native receive report: {cause:?}"
        );
    }
}

#[test]
fn response_scenarios() {
    for (mode, rounds, tries, want) in [
        (FILTER, 2, 1, Ok(1)),
        (ALIAS, 2, 1, Ok(2)),
        (
            Mode::Reply(Reply::Aliases([
                b"\x04edge\x07example\0",
                b"\x04EDGE\x07EXAMPLE\0",
            ])),
            2,
            1,
            Ok(2),
        ),
        (
            Mode::Stream(Reply::Aliases([
                b"\x04edge\x07example\0",
                b"\x04next\x07example\0",
            ])),
            2,
            1,
            Err(Kind::Malformed(Malformed::AliasConflict)),
        ),
        (
            Mode::Stream(Reply::Aliases([
                b"\x04next\x07example\0",
                b"\x04edge\x07example\0",
            ])),
            2,
            1,
            Err(Kind::Malformed(Malformed::AliasConflict)),
        ),
        (Mode::Follow(b"\x04edge\xc0\x10"), 3, 1, Ok(12)),
        (
            Mode::Reply(Reply::Alias(None, b"\0")),
            2,
            1,
            Err(Kind::Malformed(Malformed::Name(host::Error::NoLabels))),
        ),
        (
            Mode::Reply(Reply::Alias(None, b"\x04-bad\xc0\x10")),
            2,
            1,
            Err(Kind::Malformed(Malformed::Name(host::Error::LabelHyphen {
                index: 0,
                offset: 0,
            }))),
        ),
        (
            Mode::Reply(Reply::Alias(None, b"\x04-bad\x07example\0")),
            2,
            1,
            Err(Kind::Malformed(Malformed::Name(host::Error::LabelHyphen {
                index: 0,
                offset: 0,
            }))),
        ),
        (
            Mode::Reply(Reply::Alias(None, b"\x04b!ad\x07example\0")),
            2,
            1,
            Err(Kind::Malformed(Malformed::Name(
                host::Error::LabelCharacter {
                    index: 0,
                    offset: 1,
                    value: b'!',
                },
            ))),
        ),
        (FULL, 2, 1, CAPACITY),
        (Mode::Refused, 3, 2, Ok(4)),
        (Mode::Reply(Reply::Name), 2, 1, Err(Kind::Name)),
        (Mode::Malformed(true), 2, 2, Ok(3)),
        (
            Mode::Malformed(false),
            2,
            1,
            Err(Kind::Malformed(Malformed::Packet(Packet::Truncated {
                offset: 2,
                required: 2,
                actual: 0,
            }))),
        ),
        (Mode::Close(true), 2, 2, Ok(6)),
        (
            Mode::Close(false),
            2,
            1,
            Err(Kind::Close {
                reason: close::Reason::Remote,
            }),
        ),
        (Mode::Partial, 2, 2, Ok(7)),
        (Mode::Cookie(Cookie::Missing), 2, 1, Ok(8)),
        (
            Mode::ExtendedCode(0x80),
            2,
            1,
            Err(Kind::Server { code: 2048 }),
        ),
        (Mode::Reject(Reject::Wrong), 2, 1, Ok(10)),
        (Mode::Reject(Reject::Duplicate), 2, 1, Ok(10)),
        (Mode::Reject(Reject::Trailing), 2, 1, Ok(10)),
        (Mode::Reject(Reject::Forward), 2, 1, Ok(10)),
        (Mode::Reject(Reject::AliasConflict), 2, 1, Ok(10)),
    ] {
        let server = (Peer::new(mode, rounds)).expect("DNS fixture server must start");
        let servers =
            config::Servers::try_new([server.addr]).expect("fixture server list must be valid");
        let ((out, start), worker) = server
            .with(|| {
                let start = Instant::now();
                (
                    resolve(
                        "api.example:443",
                        servers,
                        tries,
                        Duration::from_secs(1),
                        Duration::from_secs(u64::from(tries)),
                    ),
                    start,
                )
            })
            .expect("DNS worker start");
        worker
            .expect("DNS worker joined")
            .expect("DNS fixture server must finish");
        check(out, want);
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "retry waited for timeout"
        );
    }
}

#[test]
fn retries_bad_cookie_once() {
    for mode in [Cookie::Bad, Cookie::Renew] {
        let server = Peer::new(Mode::Cookie(mode), 4).expect("start cookie challenge peer");
        let servers = config::Servers::try_new([server.addr]).expect("cookie challenge server");
        let (result, worker) = server
            .with(|| {
                resolve(
                    "api.example:443",
                    servers,
                    1,
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                )
            })
            .expect("cookie worker start");
        check(result, Ok(8));
        assert_eq!(
            worker.expect("DNS worker joined").expect("cookie exchange"),
            4
        );
    }
}

#[test]
fn renews_wildcard_cookie() {
    let server = Peer::new(Mode::Cookie(Cookie::Renew), 4).expect("start wildcard cookie peer");
    let servers = config::Servers::try_new([server.addr]).expect("wildcard cookie server");
    let mut config = fixture::configure(servers, 1);
    config.bind = SocketAddr::from(([0, 0, 0, 0], 0));
    let ((result, received, _), worker) = server
        .with(|| resolve_configured::<1>("api.example:443", config))
        .expect("wildcard cookie worker start");
    check(result, Ok(8));
    assert!(received.is_empty(), "wildcard source receive failed");
    assert_eq!(
        worker.expect("DNS worker joined").expect("cookie exchange"),
        4
    );
}

#[test]
fn bounds_alias_deadline() {
    let delay = Duration::from_millis(35);
    let server = (Peer::new(Mode::Delay(delay), 3)).expect("DNS fixture server must start");
    let servers =
        config::Servers::try_new([server.addr]).expect("fixture server list must be valid");
    let attempt = Duration::from_millis(120);
    let total = Duration::from_millis(260);
    let ((out, elapsed), worker) = server
        .with(|| {
            let start = Instant::now();
            let out = resolve("api.example:443", servers, 3, attempt, total);
            (out, start.elapsed())
        })
        .expect("DNS worker start");
    let count = worker
        .expect("DNS worker joined")
        .expect("DNS fixture server must finish");
    check(out, Err(Kind::Timeout));
    assert!(count >= 3, "alias chain did not span multiple hops");
    assert!(
        elapsed + Duration::from_millis(60) >= total,
        "deadline fired early"
    );
    assert!(
        elapsed < total + Duration::from_millis(450),
        "alias extended deadline"
    );
}

#[test]
fn kernel_failures() {
    let bad = SocketAddr::from(([255, 255, 255, 255], 53));
    let limit = Duration::from_secs(2);
    let start = Instant::now();
    let servers = config::Servers::try_new([bad]).expect("fixture server list must be valid");
    let out = resolve("api.example:443", servers, 1, limit, limit);
    let elapsed = start.elapsed();
    let errno = match out {
        Err(Error::Query {
            kind: Kind::Transport { errno },
            ..
        }) => Some(errno),
        _ => None,
    }
    .expect("kernel failure must preserve its numeric errno");

    assert!(
        errno == Errno::try_from(libc::EACCES).expect("errno")
            || errno == Errno::try_from(libc::EPERM).expect("errno")
    );
    assert!(elapsed < limit, "kernel failure waited for timeout");

    static NEXT: [[u8; 4]; 1] = [[192, 0, 2, 5]];
    let server = (Peer::new(Mode::Reply(Reply::Addrs(&NEXT, false, false)), 2))
        .expect("DNS fixture server must start");
    let list =
        (config::Servers::try_new([bad, server.addr])).expect("fixture server list must be valid");
    let ((out, elapsed), worker) = server
        .with(|| {
            let start = Instant::now();
            let out = resolve("api.example:443", list, 2, limit, limit * 2);
            (out, start.elapsed())
        })
        .expect("DNS worker start");
    worker
        .expect("DNS worker joined")
        .expect("DNS fixture server must finish");
    check(out, Ok(5));
    assert!(elapsed < limit, "kernel retry waited for timeout");
}

#[test]
fn cycles_servers() {
    for count in 1..=4 {
        let server = Peer::new(Mode::Refused, 3).expect("start cyclic resolver fixture");
        let addrs = iter::once(server.addr)
            .chain((1..count).map(|offset| SocketAddr::from(([255, 255, 255, 255], 53 + offset))));
        let servers = config::Servers::try_new(addrs).expect("distinct configured servers");
        let limit = Duration::from_secs(1);
        let (out, worker) = server
            .with(|| {
                resolve(
                    "api.example:443",
                    servers,
                    count as u8 + 1,
                    limit,
                    limit * u32::from(count + 1),
                )
            })
            .expect("cyclic resolver worker start");
        assert_eq!(
            worker
                .expect("DNS worker joined")
                .expect("complete cyclic resolver fixture"),
            3
        );
        check(out, Ok(4));
    }
}

#[test]
fn fills_maximum_query() {
    let target = format!(
        "{}.{}.{}.{}:443",
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(61),
    );
    for (mode, rounds, want) in [(Mode::Refused, 3, 4), (Mode::Partial, 2, 7)] {
        let server = (Peer::new(mode, rounds)).expect("the maximum-name peer must start");
        let servers = (config::Servers::try_new([server.addr]))
            .expect("the local peer is a valid DNS server");
        let (out, worker) = server
            .with(|| {
                resolve(
                    &target,
                    servers,
                    2,
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                )
            })
            .expect("maximum-name worker start");
        worker
            .expect("DNS worker joined")
            .expect("the peer must observe the complete maximum-name query");
        check(out, Ok(want));
    }
}

fn check(out: Result<Option<SocketAddr>, Error<1>>, want: Result<u8, Kind>) {
    match want {
        Ok(last) => {
            let address = out.expect("resolved address");
            assert_eq!(address, Some(SocketAddr::from(([192, 0, 2, last], 443))));
        }
        Err(kind) => {
            assert!(matches!(out, Err(Error::Query { kind: actual, .. }) if actual == kind));
        }
    }
}

fn resolve(
    target: &str,
    servers: config::Servers,
    tries: u8,
    attempt: Duration,
    total: Duration,
) -> Result<Option<SocketAddr>, Error<1>> {
    let (result, received, _) = resolve_observed(target, servers, tries, attempt, total);
    assert!(
        received.is_empty(),
        "unexpected DNS receive failure: {received:?}"
    );
    result
}

fn resolve_observed(
    target: &str,
    servers: config::Servers,
    tries: u8,
    attempt: Duration,
    total: Duration,
) -> (
    Result<Option<SocketAddr>, Error<1>>,
    Vec<ReceiveError>,
    settings::Receive,
) {
    resolve_configured::<1>(target, lookup::configure(servers, tries, attempt, total))
}

fn resolve_configured<const LANES: usize>(
    target: &str,
    cfg: config::Config,
) -> (
    Result<Option<SocketAddr>, Error<1>>,
    Vec<ReceiveError>,
    settings::Receive,
) {
    let mut finish = Completion::new();
    let mut received = Vec::new();
    let ready = fixture::try_prepare::<4, LANES, 1>(cfg, &mut received).expect("DNS layout");
    let receive = ready.layout().receive();
    let mut resolved = None;
    ready.try_scoped(
        |scope| {
            let result = scope.lend(|install, stage| {
                lending::Installer::install(install, stage, Query::<LANES>(target, &mut finish))
            });
            resolved = Some(result.map_err(App::Install).flatten());
        },
        Policy::new(
            |report| fixture::reclaim(report).expect("DNS reclaim"),
            |failure: Recovery<'_, '_>| -> Infallible {
                panic!("DNS recovery: {:?}", failure.cause())
            },
        ),
        |entered| {
            assert!(
                entered.is_ok(),
                "DNS scope: {:?}",
                entered.as_ref().err().map(|error| error.kind())
            );
        },
    );

    let resolved = resolved
        .expect("DNS scope ran")
        .expect("DNS lookup application");
    (resolved, received, receive)
}
