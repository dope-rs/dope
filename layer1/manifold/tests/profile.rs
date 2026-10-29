pub mod common;

use std::mem;

use dope_core::driver::{
    route::KeyTag,
    settings::{Config, Domain, Progress},
};
use dope_manifold::{
    app::{env::Bundle, receive},
    network::{datagram::plan, listener::topology},
    timing::{Balanced, Throughput},
};
use dope_net::{tcp::Tcp, wire::Identity};

use crate::common::manifold::profile::{
    DriverOnly, InputFactory, TimingOnly, listener_config, require_split,
};

type Environment = Bundle<Tcp, Identity, DriverOnly, Balanced>;

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
fn bundle_types() {
    type Split = Bundle<Tcp, Identity, DriverOnly, TimingOnly>;
    require_split::<Split>();
    assert_eq!(mem::size_of::<Split>(), 0);
}

#[test]
fn receive_domains() {
    let borrowed = topology::Plan::<KeyTag<0>, Environment, _>::try_new(
        listener_config(),
        InputFactory::<receive::Borrowed>::NEW,
    );
    let coupled = topology::Plan::<KeyTag<0>, Environment, _>::try_new(
        listener_config(),
        InputFactory::<receive::Coupled>::NEW,
    );
    let gated = topology::Plan::<KeyTag<0>, Environment, _>::try_new(
        listener_config(),
        InputFactory::<receive::Gated>::NEW,
    );
    let owned = topology::Plan::<KeyTag<0>, Environment, _>::try_new(
        listener_config(),
        InputFactory::<receive::Owned>::NEW,
    );
    let scoped = topology::Plan::<KeyTag<0>, Environment, _>::try_new(
        listener_config(),
        InputFactory::<receive::Scoped>::NEW,
    );

    let borrowed = (borrowed).expect("required fixture outcome");
    let coupled = (coupled).expect("required fixture outcome");
    let gated = (gated).expect("required fixture outcome");
    let owned = (owned).expect("required fixture outcome");
    let scoped = (scoped).expect("required fixture outcome");
    assert!(DriverOnly::try_domains(borrowed) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(coupled) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(gated) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(owned) == Ok(Domain::ZERO));
    assert!(DriverOnly::try_domains(scoped) == Ok(Domain::ONE));

    let first = topology::Plan::<KeyTag<0>, Environment, _>::try_new(
        listener_config(),
        InputFactory::<receive::Scoped>::NEW,
    );
    let second = topology::Plan::<KeyTag<1>, Environment, _>::try_new(
        listener_config(),
        InputFactory::<receive::Scoped>::NEW,
    );

    let first = (first).expect("required fixture outcome");
    let second = (second).expect("required fixture outcome");
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
