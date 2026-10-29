pub mod common;

use std::time::Duration;

use crate::common::refresh::{
    BOTH, FIRST, NEXT,
    server::{self, Stub},
};

#[test]
fn pending_datagram_stops_without_refresh() {
    use std::convert::Infallible;

    use dope::{
        core::driver::life::recovery::{Policy, Recovery},
        manifold::network::datagram::RejectCause,
        runtime::executor::session::Completion,
    };
    use dope_dns::{Target, config, lookup::error};

    use crate::common::{fixture, refresh};

    let servers = config::Servers::try_new([fixture::SERVER]).expect("DNS server");
    let mut config = fixture::configure(servers, 1);
    config.query.attempt_timeout = Duration::from_secs(60);
    config.query.total_timeout = Duration::from_secs(60);
    let run = refresh::Run::new(false, 1);
    let mut received = Vec::new();
    let prepared = fixture::try_prepare::<
        { refresh::OUTBOUND },
        { refresh::LANES },
        { refresh::ADDRESSES },
    >(config, &mut received)
    .expect("DNS resources");
    let mut finish = Completion::new();
    prepared.try_scoped(
        |scope| {
            scope.lend(|install, stage| {
                refresh::stop(
                    refresh::Stop {
                        target: Target::parse("pending.example:443").expect("DNS target"),
                        run: run.clone(),
                    },
                    &mut finish,
                    install,
                    stage,
                );
            });
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
    assert!(matches!(
        run.fault.take(),
        Some(refresh::Advance::Query(error::Error::Query {
            kind: error::Kind::Datagram(error::Datagram::Rejected(RejectCause::Inactive)),
            ..
        }))
    ));
    assert_eq!(run.failures.get(), 1);
    assert!(received.is_empty(), "unexpected DNS receive: {received:?}");
}

#[test]
fn datagram_atomic() {
    basic(false).expect("DNS refresh server");
}

#[test]
fn stream_retry() {
    basic(true).expect("DNS refresh server");
}

#[test]
fn retains_cookie_after_stream() {
    let run = Stub::new(false, true, 0, &[30], None)
        .missing_cookie()
        .run(200, 200, 3, None)
        .expect("DNS cookie retention");
    assert_eq!(run.published.take().len(), 3);
    let mut resolved = run.resolved.take();
    resolved[1].sort();
    assert_eq!(resolved[1], BOTH);
}

#[test]
fn renews_cached_cookie() {
    let run = Stub::new(false, true, 0, &[30], None)
        .renew_cookie()
        .run(200, 200, 3, None)
        .expect("DNS cookie renewal");
    assert_eq!(run.published.take().len(), 3);
}

#[test]
fn ttl_replaces() {
    let sets = Stub::new(false, true, 0, &[30], None)
        .run(200, 200, 2, None)
        .expect("DNS refresh server")
        .resolved
        .take();
    let mut next = sets[1].clone();
    next.sort();
    assert_eq!(sets[0], FIRST);
    assert_eq!(next, NEXT);
}

#[test]
fn zero_ttl_keeps() {
    let run = Stub::new(false, true, 0, &[0], None)
        .run(60_000, 50, 2, None)
        .expect("DNS refresh server");
    let sets = run.resolved.take();
    assert_eq!(run.failures.get(), 0);
    assert_eq!(sets[0], FIRST);
    assert_eq!(sets[1], NEXT);
}

#[test]
fn ttl_high_bit_zero() {
    let meta = Stub::new(false, true, 0, &[0x8000_0000], None)
        .run(60_000, 50, 1, None)
        .expect("DNS refresh server")
        .published
        .take();
    assert_eq!(meta[0].ttl(), Duration::ZERO);
}

#[test]
fn ttl_duplicate_min() {
    let meta = Stub::new(false, true, 0, &[30, 1], None)
        .run(60_000, 50, 1, None)
        .expect("DNS refresh server")
        .published
        .take();
    assert_eq!(meta[0].ttl(), Duration::from_secs(1));
}

#[test]
fn alias_ttl_ages() {
    let meta = Stub::new(false, true, 0, &[30], Some(&[30, 1]))
        .run(60_000, 50, 1, Some(Duration::from_millis(500)))
        .expect("DNS refresh server")
        .published
        .take();
    assert_eq!(meta[0].ttl(), Duration::from_millis(500));
}

#[test]
fn alias_expired_completes() {
    let meta = Stub::new(false, true, 0, &[30], Some(&[1]))
        .run(60_000, 50, 1, Some(Duration::from_millis(1500)))
        .expect("DNS refresh server")
        .published
        .take();
    assert_eq!(meta[0].ttl(), Duration::ZERO);
}

#[test]
fn family_ttl_ages() {
    let meta = Stub::new(false, false, 0, &[1], Some(&[30]))
        .run(60_000, 50, 1, Some(Duration::from_millis(500)))
        .expect("DNS refresh server")
        .published
        .take();
    assert_eq!(meta[0].ttl(), Duration::from_millis(500));
}

#[test]
fn stale_recovers() {
    let run = Stub::new(false, true, 2, &[30], None)
        .run(20, 100, 2, None)
        .expect("DNS refresh server");
    let sets = run.resolved.take();
    let mut next = sets[1].clone();
    next.sort();
    assert!(run.failures.get() > 0);
    assert_eq!(sets[0], FIRST);
    assert_eq!(next, NEXT);
}

#[test]
fn stale_expires() {
    let run = Stub::new(false, true, u8::MAX, &[30], None)
        .run(20, 20, 2, None)
        .expect("DNS refresh server");
    let sets = run.resolved.take();
    assert!(run.failures.get() > 0);
    assert_eq!(sets[0], FIRST);
    assert!(sets[1].is_empty());
}

fn basic(truncate: bool) -> Result<(), server::Failure> {
    let mut values = Stub::new(truncate, false, 0, &[30], None)
        .run(60_000, 60_000, 1, None)?
        .resolved
        .take()[0]
        .clone();
    values.sort();
    assert_eq!(values, BOTH);
    Ok(())
}
