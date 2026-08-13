use std::{error, io, net, num};

use dope::manifold::service;
use dope_dns::{Name, Storage, Target, config, query};

use crate::fixture;

#[test]
fn name_is_canonical_and_bounded() {
    let name = Name::parse("API.Example.").unwrap();
    assert_eq!(name.to_string(), "api.example");
    assert_eq!(
        Name::parse("API.Ex_ample.").unwrap().to_string(),
        "api.ex_ample"
    );
    assert!(Name::parse("").is_err());
    assert!(Name::parse("-api.example").is_err());
    assert!(Name::parse("api..example").is_err());
    assert!(Name::parse(&format!("{}.example", "a".repeat(64))).is_err());
}

#[test]
fn target_distinguishes_names_and_addresses() {
    let hostname = Target::parse("api.example:443").unwrap();
    assert_eq!(hostname.name().unwrap().to_string(), "api.example");
    assert_eq!(hostname.port(), 443);
    assert_eq!(hostname.ip(), None);

    let address = net::SocketAddr::from(([10, 1, 105, 164], 8443));
    let literal = Target::from(address);
    assert_eq!(literal.name(), None);
    assert_eq!(literal.ip(), Some(address.ip()));
    assert_eq!(literal.to_string(), address.to_string());
}

#[test]
fn invalid_target_port_preserves_its_parse_source() {
    let failure = Target::parse("api.example:not-a-port").expect_err("port must be rejected");

    assert_eq!(failure.kind(), io::ErrorKind::InvalidInput);
    let source = failure.get_ref().expect("typed target error");
    assert!(error::Error::source(source).is_some_and(|cause| cause.is::<num::ParseIntError>()));
    assert!(
        failure
            .to_string()
            .starts_with("invalid service target port:")
    );
}

#[test]
fn servers_enforce_the_fixed_bound() {
    let addresses = (1..=5).map(|last| net::SocketAddr::from(([127, 0, 0, last], 53)));
    assert!(config::Servers::try_from_iter(addresses).is_err());
}

#[test]
fn config_rejects_mixed_socket_families() {
    let servers =
        config::Servers::try_from_iter([net::SocketAddr::from((net::Ipv6Addr::LOCALHOST, 53))])
            .unwrap();
    assert!(fixture::config(servers, 2).validate().is_err());
}

#[test]
fn storage_rejects_invalid_lane_bounds() {
    assert!(Storage::<0, 4>::new(fixture::config(config::Servers::empty(), 2)).is_err());
    assert!(Storage::<32_768, 1>::new(fixture::config(config::Servers::empty(), 2)).is_err());
}

#[test]
fn storage_accepts_the_service_endpoint_ceiling() {
    Storage::<1, { service::MAX_ENDPOINTS }>::new(fixture::config(config::Servers::empty(), 2))
        .unwrap();
}

#[test]
fn only_literal_targets_bypass_transport_binding() {
    let storage = Storage::<1, 4>::new(fixture::config(config::Servers::empty(), 2)).unwrap();
    let hostname = Target::parse("api.example:443").unwrap();
    assert!(storage.literal(hostname).is_err());

    let address = net::SocketAddr::from(([127, 0, 0, 1], 443));
    let discovery = storage.literal(Target::from(address)).unwrap();
    assert_eq!(discovery.target().ip(), Some(address.ip()));
}

#[test]
fn nominal_policy_has_no_runtime_storage_cost() {
    #[allow(dead_code)]
    struct RuntimeConfig {
        bind: net::SocketAddr,
        servers: config::Servers,
        query: config::Query,
        refresh: config::Refresh,
    }

    assert_eq!(std::mem::size_of::<query::Policy>(), 0);
    assert_eq!(
        std::mem::size_of::<config::Config>(),
        std::mem::size_of::<RuntimeConfig>()
    );
}

#[test]
fn discovery_error_kind_path_is_preserved() {
    assert_eq!(
        dope_dns::discovery::ErrorKind::Timeout,
        dope_dns::discovery::ErrorKind::Timeout
    );
}
