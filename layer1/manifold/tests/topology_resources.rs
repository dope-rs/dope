pub mod common;

use std::{net::SocketAddr, time::Duration};

use dope_core::driver::{
    route,
    route::KeyTag,
    settings,
    settings::{
        claim::{Claim, Needs},
        errors,
    },
};
use dope_manifold::{
    app::env,
    client::session::codec::staging,
    network::client::{topology, topology::plan},
    service, timing,
};
use dope_net::{tcp, wire};

use crate::common::manifold::Topology;
#[test]
fn connector_claims_shape() {
    let set = service::Snapshot::<service::Endpoint<SocketAddr, SocketAddr>>::empty(
        service::Revision::INITIAL,
    );
    let node = {
        (topology::Install::<
            KeyTag<7>,
            3,
            env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
            _,
        >::new(
            set,
            3,
            plan::Spec::new(Duration::from_millis(1), Duration::from_secs(1))
                .expect("valid retry bounds"),
            (),
        ))
        .expect("connector capacity must fit its maximum: {kind:?}")
    };
    let needs = { (Needs::from_tree(&node)).expect("connector claims must be unique: {kind:?}") };
    let layout = {
        (needs.resolve::<Topology>()).expect("connector claims must fit the test profile: {kind:?}")
    };

    assert!(matches!(
        needs.include(Claim::new(settings::Routes::singleton::<KeyTag<7>>())),
        Err(errors::Claim::DuplicateRoute(id)) if id == route::Id::fixed::<7>()
    ));
    assert_eq!(layout.file_slots().outbound().count().get(), 3);
    assert!((layout.domains()) == (settings::Domain::fixed::<2>()));
    assert_eq!(layout.schedule().ready().count().get(), 1);
    assert_eq!(layout.schedule().timers().count().get(), 3);
}

#[test]
fn inputs_claim_domains() {
    assert!(Topology::try_domains::<plan::Local>() == Ok(settings::Domain::fixed::<2>()));
    assert!(Topology::try_domains::<plan::Local<staging::Direct>>() == Ok(settings::Domain::ONE));
    assert!(Topology::try_domains::<plan::External>() == Ok(settings::Domain::ONE));
    assert!(
        Topology::try_domains::<plan::External<staging::Direct>>() == Ok(settings::Domain::ZERO)
    );
}

#[test]
fn aux_claims_sockets() {
    let set = service::Snapshot::<service::Endpoint<SocketAddr, SocketAddr>>::empty(
        service::Revision::INITIAL,
    );
    let plan = plan::Plan::<KeyTag<8>, 3, plan::Side>::try_new(
        3,
        plan::Spec::new(Duration::from_millis(1), Duration::from_secs(1))
            .expect("valid retry bounds"),
    )
    .expect("side connector capacity must fit its maximum");
    let node = topology::Install::<
        KeyTag<8>,
        3,
        env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
        _,
        plan::Side,
    >::from_plan(set, plan, ());
    let needs =
        { (Needs::from_tree(&node)).expect("side connector claims must be unique: {kind:?}") };
    let layout = {
        (needs.resolve::<Topology>())
            .expect("side connector claims must fit the test profile: {kind:?}")
    };

    assert_eq!(layout.file_slots().outbound().count().get(), 6);
}

#[test]
fn capacity_fails_early() {
    let set = service::Snapshot::<service::Endpoint<SocketAddr, SocketAddr>>::empty(
        service::Revision::INITIAL,
    );
    let node = topology::Install::<
        KeyTag<7>,
        1,
        env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
        _,
    >::new(
        set,
        2,
        plan::Spec::new(Duration::from_millis(1), Duration::from_secs(1))
            .expect("valid retry bounds"),
        (),
    );

    assert!(matches!(
        node,
        Err(errors::Claim::DescriptorConnectionsOutOfRange {
            actual: 2,
            minimum: 1,
            maximum: 1,
        })
    ));
}
