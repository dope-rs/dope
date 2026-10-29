use dope::core::driver::route::KeyTag;

use crate::common::fixture;
pub mod common;

use std::net;

use dope::{
    core::driver::{route, settings, settings::errors::Claim},
    manifold::{self, timing::Balanced},
    net::link::egress,
    runtime::topology::Plan,
};
use dope_dns::{config, storage, topology};

#[test]
fn rejects_invalid_bounds() {
    let mut received = Vec::new();
    let result = topology::Plan::<KeyTag<0>, KeyTag<1>, 0, 1, 4, _>::new(
        egress::Config::default(),
        |cause| received.push(cause),
    );
    assert!(matches!(
        result,
        Err(topology::Prepare::Datagram(
            manifold::ConfigError::DatagramPendingZero
        ))
    ));

    let result = topology::Plan::<KeyTag<0>, KeyTag<1>, { usize::MAX }, 1, 4, _>::new(
        egress::Config::default(),
        |cause| received.push(cause),
    );
    assert!(matches!(
        result,
        Err(topology::Prepare::Config(error))
            if std::matches!(
                error,
                config::Error::OutboundBytes {
                    outbound: usize::MAX,
                    per_query: bytes,
                } if bytes > 1
            )
    ));
    assert!(
        received.is_empty(),
        "uninstalled topology received a packet: {received:?}"
    );
}

#[test]
fn rejects_stream_egress() {
    let mut received = Vec::new();
    let retained = egress::Config::shared(egress::Credit::fixed::<1, 0>());
    let result = topology::Plan::<KeyTag<0>, KeyTag<1>, 1, 1, 4, _>::new(retained, |cause| {
        received.push(cause)
    });
    assert!(matches!(
        result,
        Err(topology::Prepare::Egress {
            required_entries,
            available_entries,
            required_resident,
            available_resident,
        }) if required_entries == egress::Entries::new(1)
            && available_entries == egress::Entries::new(1)
            && required_resident > egress::Resident::new(0)
            && available_resident == egress::Resident::new(0)
    ));
    assert!(
        received.is_empty(),
        "uninstalled topology received a packet: {received:?}"
    );
}

#[test]
fn claims_resident_domains() {
    let mut received = Vec::new();
    let servers = (config::Servers::try_new([net::SocketAddr::from(([127, 0, 0, 1], 53))]))
        .expect("test resolver server must construct");
    let config = fixture::configure(servers, 1);
    let plan = (topology::Plan::<KeyTag<0>, KeyTag<1>, 1, 1, 4, _>::new(
        egress::Config::default(),
        |cause| received.push(cause),
    ))
    .expect("test DNS topology must construct");
    let storage = (storage::Plan::<1, 4>::new(config)).expect("test DNS storage must construct");
    let needs = settings::claim::Needs::from_tree(&plan).expect("DNS resource claim");
    let baseline = settings::Config::for_profile::<Balanced>()
        .with_receive(settings::Receive::fixed::<2, 512>());
    let admitted = needs.apply_to(baseline).expect("DNS receive floor");
    assert!(admitted.receive() == settings::Datagram::fixed::<2, 1232>().receive());
    {
        let ready = (Plan::new(storage, plan).for_install::<Balanced>())
            .expect("test DNS install plan must construct");
        assert!((ready.layout().domains()) == (settings::Domain::fixed::<3>()));
        assert!(ready.layout().receive() == settings::Config::for_profile::<Balanced>().receive());
    }
    assert!(
        received.is_empty(),
        "uninstalled topology received a packet: {received:?}"
    );
}

#[test]
fn rejects_duplicate_routes() {
    let mut received = Vec::new();
    let servers = (config::Servers::try_new([net::SocketAddr::from(([127, 0, 0, 1], 53))]))
        .expect("test resolver server must construct");
    let storage = (storage::Plan::<1, 4>::new(fixture::configure(servers, 1)))
        .expect("test DNS storage must construct");
    let plan = (topology::Plan::<KeyTag<7>, KeyTag<7>, 1, 1, 4, _>::new(
        egress::Config::default(),
        |cause| received.push(cause),
    ))
    .expect("duplicate-route topology must construct");
    let result = Plan::new(storage, plan).for_install::<Balanced>();
    assert!(matches!(
        result,
        Err(Claim::DuplicateRoute(id)) if id == route::Id::fixed::<7>()
    ));
    assert!(
        received.is_empty(),
        "uninstalled topology received a packet: {received:?}"
    );
}
