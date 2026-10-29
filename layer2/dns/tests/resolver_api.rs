use std::convert::Infallible;

use dope::{
    core::driver::life::{
        creation::Attempt,
        recovery::{Policy, Recovery},
    },
    runtime::executor::session::Scope,
};
use dope_test::scope::Scoped;
pub mod common;

use std::{mem, net, num::IntErrorKind, time};

use dope::{
    core::driver::settings,
    manifold::{service, timing::Throughput},
    runtime::executor::Executor,
};
use dope_dns::{
    HostRef, Target, config,
    host::{self, Name},
    lookup::{create, refresh::Transport},
    storage,
};
use dope_test::alloc::Tracker;
use storage::literal;

use crate::common::fixture;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn name_is_canonical() {
    let name = (Name::parse("API.Example.")).expect("canonical hostname must parse");
    assert_eq!(name.as_str(), "api.example");
    let parsed = (Name::parse("API.Ex_ample.")).expect("underscore hostname must parse");
    assert_eq!(parsed.as_str(), "api.ex_ample");
    assert!(matches!(
        Name::parse(""),
        Err(host::Error::NameLength {
            actual: 0,
            minimum: 1,
            maximum: 253,
        })
    ));
    assert!(matches!(
        Name::parse("-api.example"),
        Err(host::Error::LabelHyphen {
            index: 0,
            offset: 0,
        })
    ));
    assert!(matches!(
        Name::parse("api..example"),
        Err(host::Error::LabelLength {
            index: 1,
            actual: 0,
            minimum: 1,
            maximum: 63,
        })
    ));
    assert!(matches!(
        Name::parse(&format!("{}.example", "a".repeat(64))),
        Err(host::Error::LabelLength {
            index: 0,
            actual: 64,
            minimum: 1,
            maximum: 63,
        })
    ));
    assert!(matches!(
        Name::parse("api!.example"),
        Err(host::Error::LabelCharacter {
            index: 0,
            offset: 3,
            value: b'!',
        })
    ));
}

#[test]
fn target_kinds_differ() {
    let hostname = (Target::parse("api.example:443")).expect("hostname target must parse");
    assert!(hostname.is_name());
    assert!(
        matches!(hostname.as_ref().host(), HostRef::Name(name) if name.as_str() == "api.example")
    );
    assert_eq!(hostname.as_ref().port().get(), 443);

    let address = net::SocketAddr::V6(net::SocketAddrV6::new(
        net::Ipv6Addr::LOCALHOST,
        8443,
        19,
        7,
    ));
    let literal = (Target::try_from(address)).expect("socket address target must construct");
    assert!(!literal.is_name());
    assert!(matches!(literal.as_ref().host(), HostRef::Address(addr) if addr == address));
    assert_eq!(literal.as_ref().port().get(), address.port());
}

#[test]
fn parts_preserve_ipv6() {
    let target = (Target::try_new("2001:db8::1", 443)).expect("IPv6 target must construct");
    assert!(!target.is_name());
    assert!(
        matches!(target.as_ref().host(), HostRef::Address(net::SocketAddr::V6(addr)) if addr.ip().segments() == [0x2001, 0xdb8, 0, 0, 0, 0, 0, 1])
    );
    assert_eq!(target.as_ref().port().get(), 443);
    assert_eq!(
        Target::parse("[2001:db8::1]:443").expect("bracketed IPv6 target must parse"),
        target
    );
}

#[test]
fn unbracketed_ipv6_is_rejected() {
    for input in ["::1", "::2", "2001:db8::1:443", "api.example:443:53"] {
        assert_eq!(
            Target::parse(input),
            Err(dope_dns::TargetError::BracketedPort)
        );
    }
}

#[test]
fn scoped_literal_parses() {
    let target = Target::parse("[fe80::1%7]:443").expect("scoped IPv6 target must parse");
    assert!(matches!(
        target.as_ref().host(),
        HostRef::Address(net::SocketAddr::V6(address)) if address.scope_id() == 7
    ));
}

#[test]
fn zero_port_is_rejected() {
    assert!(Target::try_new("example.test", 0).is_err());
    assert!(Target::parse("example.test:0").is_err());

    let address = net::SocketAddr::from(([127, 0, 0, 1], 0));
    let result = Target::try_from(address);
    assert!(matches!(result, Err(dope_dns::TargetError::ZeroPort)));
}

#[test]
fn invalid_port_cause() {
    let result = Target::parse("api.example:not-a-port");
    assert!(matches!(
        result,
        Err(dope_dns::TargetError::InvalidPort(error))
            if error.kind() == &IntErrorKind::InvalidDigit
    ));
}

#[test]
fn clone_costs_are_exact() {
    let named = (Target::parse("api.example:443")).expect("hostname target must parse");
    let (copy, count) = Tracker::count(|| named.try_clone());
    let copy = (copy).expect("hostname target clone must allocate");
    assert_eq!(count, 1);
    assert!(copy == named);

    let literal = (Target::try_from(net::SocketAddr::from(([127, 0, 0, 1], 443))))
        .expect("literal target must construct");
    let (copy, count) = Tracker::count(|| literal.try_clone());
    let copy = (copy).expect("literal target clone must remain representable");
    assert_eq!(count, 0);
    assert!(copy == literal);
}

#[test]
fn servers_are_bounded() {
    let addrs = (1..=5).map(|last| net::SocketAddr::from(([127, 0, 0, last], 53)));
    assert!(config::Servers::try_new(addrs).is_err());
}

#[test]
fn selection_reports_bounds() {
    let empty = config::Servers::empty();
    assert_eq!(
        empty.try_get(0),
        Err(config::IndexError { index: 0, count: 0 })
    );
    let addrs = [
        net::SocketAddr::from(([127, 0, 0, 1], 53)),
        net::SocketAddr::from(([127, 0, 0, 2], 53)),
    ];
    let servers = config::Servers::try_new(addrs).expect("valid distinct resolver addresses");
    let first = servers.try_get(0).expect("first configured server");
    let last = servers.try_get(1).expect("last configured server");
    assert_eq!((*first, *last), (addrs[0], addrs[1]));
    assert_eq!(
        servers.try_get(2),
        Err(config::IndexError { index: 2, count: 2 })
    );
    assert_eq!(
        servers.try_get(usize::MAX),
        Err(config::IndexError {
            index: usize::MAX,
            count: 2
        })
    );
}

#[test]
fn duplicate_is_exact() {
    let server = net::SocketAddr::from(([127, 0, 0, 1], 53));
    let result = config::Servers::try_new([server, server]);
    assert!(matches!(
        result,
        Err(config::Error::ServerDuplicate {
            first: 0,
            duplicate: 1,
            server: seen,
        }) if seen == server
    ));
}

#[test]
fn config_rejects_mix() {
    let bind = net::SocketAddr::from(([127, 0, 0, 1], 0));
    let server = net::SocketAddr::from((net::Ipv6Addr::LOCALHOST, 53));
    let servers =
        (config::Servers::try_new([server])).expect("IPv6 resolver server must construct");
    let result = storage::Plan::<1, 4>::new(fixture::configure(servers, 2));
    assert!(matches!(
        result,
        Err(storage::Error::Config(config::Error::AddressFamily {
            bind: at,
            server: dns
        })) if at == bind && dns == server
    ));
}

#[test]
fn config_rejects_empty() {
    let result = storage::Plan::<1, 4>::new(fixture::configure(config::Servers::empty(), 2));
    assert!(matches!(
        result,
        Err(storage::Error::Config(config::Error::NoServers))
    ));
}

#[test]
fn rejects_zero() {
    let servers = (config::Servers::try_new([fixture::SERVER]))
        .expect("fixture resolver server must construct");
    let result = storage::Plan::<1, 4>::new(fixture::configure(servers, 0));
    assert!(matches!(
        result,
        Err(storage::Error::Config(config::Error::QueryAttemptsZero))
    ));
}

#[test]
fn query_budget_is_ordered() {
    let servers = (config::Servers::try_new([fixture::SERVER]))
        .expect("fixture resolver server must construct");
    let mut cfg = fixture::configure(servers, 2);
    cfg.query.attempt_timeout = time::Duration::from_secs(2);
    cfg.query.total_timeout = time::Duration::from_secs(1);
    assert!(matches!(
        storage::Plan::<1, 4>::new(cfg),
        Err(storage::Error::Config(config::Error::QueryTotalTimeoutShort { attempt, total }))
            if attempt.get() == time::Duration::from_secs(2)
                && total.get() == time::Duration::from_secs(1)
    ));
}

#[test]
fn lane_bounds_are_checked() {
    let servers = (config::Servers::try_new([fixture::SERVER]))
        .expect("fixture resolver server must construct");
    let config = fixture::configure(servers, 2);
    assert!(matches!(
        storage::Plan::<0, 4>::new(config),
        Err(storage::Error::Config(config::Error::LaneBound {
            requested: 0,
            minimum: 1,
        }))
    ));
    assert!(storage::Plan::<32_768, 1>::new(config).is_err());
}

#[test]
fn endpoint_ceiling_fits() {
    let servers = (config::Servers::try_new([fixture::SERVER]))
        .expect("fixture resolver server must construct");
    assert!(
        storage::Plan::<1, { service::MAX_ENDPOINTS }>::new(fixture::configure(servers, 2)).is_ok()
    );
}

#[test]
fn literal_uses_clock() {
    let mut create = Attempt::new();
    use dope::manifold::service::discover::{self, Action, PollAt as _, RefreshAt as _};
    use dope_dns::lookup;

    let refresh = fixture::configure(config::Servers::empty(), 2).refresh;
    let storage = literal::Plan::new(refresh).expect("literal storage refresh must validate");
    let address = net::SocketAddr::V6(net::SocketAddrV6::new(
        net::Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
        443,
        19,
        7,
    ));
    let target = Target::try_from(address).expect("literal target must construct");

    let executor = Executor::new(&mut create, settings::Config::for_profile::<Throughput>());
    let executor = executor.ok().expect("DNS test executor must construct");
    executor.with_plan(storage).run(|executor| {
        let result = executor
            .try_enter(
                Scoped::new(|mut session: Scope<'_, '_, '_, '_, literal::Plan>| {
                    let storage = session.storage();
                    {
                        let hostname = Target::parse("api.example:443").expect("hostname target must parse");
                        let failure = storage.discovery::<Transport, 1, 4>(hostname);
                        assert!(matches!(
                            failure,
                            Err(create::Error::LiteralName(target)) if target.as_ref().port().get() == 443
                                && matches!(target.as_ref().host(), HostRef::Name(name) if name.as_str() == "api.example")
                        ));

                        let address = net::SocketAddr::from(([127, 0, 0, 1], 443));
                        let target = Target::try_from(address).expect("literal target must construct");
                        let copy = target.try_clone().expect("literal target clone must remain representable");
                        let lookup = storage.discovery::<Transport, 1, 4>(copy)
                            .expect("literal discovery must have capacity");
                        let borrowed = lookup.target();
                        assert!(borrowed == target.as_ref());
                    }
                    let now = session.driver().driver_ref().clock().now();
                    let mut lookup = storage
                        .discovery::<Transport, 1, 4>(target)
                        .expect("literal discovery must have capacity");
                    lookup.refresh_at(now, discover::Reason::Startup);
                    let mut seen = false;
                    if let Action::Published {
                        snapshot, metadata, ..
                    } = lookup.poll_at(session.driver().state_token(), now)
                    {
                        seen = true;
                        assert_eq!(*snapshot.endpoints()[0].id(), address);
                        assert!(matches!(&metadata.reason, lookup::Reason::Startup));
                        assert_eq!(metadata.duration(), time::Duration::ZERO);
                    }
                    assert!(seen);
                }),
                Policy::new(
                    |report| fixture::shutdown(report).expect("DNS shutdown"),
                    |failure: Recovery<'_, '_>| -> Infallible {
                        panic!("DNS recovery: {:?}", failure.cause())
                    },
                ),
            )
            .map_err(|error| error.map_storage(|rejected| rejected.map_op(Scoped::into_inner)));
        assert!(result.is_ok());
    });
}

#[test]
fn clock_failure_stops_refresh() {
    use dope::manifold::service::discover::{self, Action, PollAt as _, RefreshAt as _};
    use dope_dns::lookup::error::{Error, Kind};

    let mut refresh = fixture::configure(config::Servers::empty(), 2).refresh;
    refresh.minimum = time::Duration::MAX;
    refresh.maximum_ttl = time::Duration::MAX;
    let storage = literal::Plan::new(refresh).expect("literal storage refresh must validate");
    let target = Target::try_new("::1", 443).expect("literal target must construct");
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, settings::Config::for_profile::<Throughput>())
        .ok()
        .expect("DNS test executor must construct");
    executor.with_plan(storage).run(|executor| {
        let result = executor
            .try_enter(
                Scoped::new(|mut session: Scope<'_, '_, '_, '_, literal::Plan>| {
                    let now = session.driver().driver_ref().clock().now();
                    let range = now
                        .try_add(time::Duration::MAX)
                        .expect_err("refresh exceeds the clock range");
                    let mut lookup = session
                        .storage()
                        .discovery::<Transport, 1, 4>(target)
                        .expect("literal discovery must have capacity");
                    lookup.refresh_at(now, discover::Reason::Startup);
                    assert!(matches!(
                        lookup.poll_at(session.driver().state_token(), now),
                        Action::Failed {
                            error: Error::Query { kind: Kind::Clock(source), .. },
                            poll_at: None,
                        } if source == range
                    ));
                    lookup.refresh_at(now, discover::Reason::Startup);
                    assert!(matches!(
                        lookup.poll_at(session.driver().state_token(), now),
                        Action::Pending { poll_at: None }
                    ));
                }),
                Policy::new(
                    |report| fixture::shutdown(report).expect("DNS shutdown"),
                    |failure: Recovery<'_, '_>| -> Infallible {
                        panic!("DNS recovery: {:?}", failure.cause())
                    },
                ),
            )
            .map_err(|error| error.map_storage(|rejected| rejected.map_op(Scoped::into_inner)));
        assert!(result.is_ok());
    });
}

#[test]
fn refresh_is_nominal() {
    use dope::manifold::service::health;
    use dope_dns::lookup::{Discovery, Reason, refresh};

    assert_eq!(
        mem::size_of::<Discovery<'static, 'static, 'static, refresh::Transport, 1, 4>>(),
        mem::size_of::<Discovery<'static, 'static, 'static, refresh::Availability, 1, 4>>()
    );
    assert!(<refresh::Transport as refresh::Policy>::reason(health::Cause::Idle).is_none());
    assert!(matches!(
        <refresh::Availability as refresh::Policy>::reason(health::Cause::Idle),
        Some(Reason::IdleFailure)
    ));
}
