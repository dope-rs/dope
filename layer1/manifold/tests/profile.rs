pub mod common;

use crate::common::manifold::profile::DriverOnly;
use dope_core::driver::{
    route::{KeyTag, table::conn::Capacity},
    settings::{Config, Domain, Progress},
};
use dope_manifold::{
    app::receive,
    network::{datagram::plan, listener::topology},
    timing::{Balanced, Throughput},
};

#[test]
fn completion_progress() {
    for mode in [
        Config::for_profile::<Balanced>().progress(),
        Config::for_profile::<Throughput>().progress(),
    ] {
        assert!(mode == Progress::BatchedWhenSupported);
    }
}

#[test]
fn receive_domains() {
    let borrowed = topology::Layout::<KeyTag<0>, receive::Borrowed>::new(Capacity::fixed::<1>());
    let coupled = topology::Layout::<KeyTag<0>, receive::Coupled>::new(Capacity::fixed::<1>());
    let gated = topology::Layout::<KeyTag<0>, receive::Gated>::new(Capacity::fixed::<1>());
    let owned = topology::Layout::<KeyTag<0>, receive::Owned>::new(Capacity::fixed::<1>());
    let scoped = topology::Layout::<KeyTag<0>, receive::Scoped>::new(Capacity::fixed::<1>());
    assert!(DriverOnly::try_domains(borrowed) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(coupled) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(gated) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(owned) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(scoped) == Ok(Domain::ONE));

    let first = topology::Layout::<KeyTag<0>, receive::Scoped>::new(Capacity::fixed::<1>());
    let second = topology::Layout::<KeyTag<1>, receive::Scoped>::new(Capacity::fixed::<1>());
    let pair = (first, second);
    assert!(DriverOnly::try_domains(pair) == Ok(Domain::fixed::<2>()));
}

#[test]
fn datagram_domains() {
    let recv = (plan::Config::new(1, 0, 1)).expect("one receive slot must be valid");
    assert!(DriverOnly::try_domains(plan::Plan::<KeyTag<0>>::new(recv)) == Ok(Domain::ZERO));

    let held = recv.retain_recv(1);
    assert!(DriverOnly::try_domains(plan::Plan::<KeyTag<0>>::new(held)) == Ok(Domain::ONE));

    let send = (plan::Config::new(1, 1, 1)).expect("one send slot must be valid");
    assert!(DriverOnly::try_domains(plan::Plan::<KeyTag<0>>::new(send)) == Ok(Domain::ONE));

    let duplex = send.retain_recv(1);
    assert!(
        DriverOnly::try_domains(plan::Plan::<KeyTag<0>>::new(duplex)) == Ok(Domain::fixed::<2>())
    );
}
