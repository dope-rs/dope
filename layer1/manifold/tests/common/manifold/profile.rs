use std::{marker, net::SocketAddr};

use dope_core::{
    driver::settings::{self, Domain, Profile, Resource, claim::Needs, errors},
    io::socket::Backlog,
};
use dope_manifold::{
    app::{
        env::{Spec, quota},
        receive,
    },
    network::listener::{config, topology::factory::ReceiveSpec},
    timing::Policy,
};
use dope_net::tcp::Tcp;
use o3::base::time::Positive;

pub struct DriverOnly;

impl Profile for DriverOnly {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
}

impl DriverOnly {
    pub fn try_domains(tree: impl Resource) -> Result<Domain, errors::Claim> {
        Ok(Needs::from_tree(&tree)?.resolve::<Self>()?.domains())
    }
}

pub struct InputFactory<I>(marker::PhantomData<fn() -> I>);

impl<I> InputFactory<I> {
    pub const NEW: Self = Self(marker::PhantomData);
}

impl<I> Resource for InputFactory<I> {
    fn include_in(&self, needs: Needs) -> Result<Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<I> ReceiveSpec for InputFactory<I>
where
    I: receive::Resources,
{
    type Input = I;
}

pub struct TimingOnly;

impl Policy for TimingOnly {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<1>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<1>();
    const SEND_DEADLINE: Positive = Positive::from_secs::<1>();
}

pub fn listener_config() -> config::Config<Tcp> {
    config::Config {
        max_connections: 1,
        per_ip: None,
        direct_flights: 1.into(),
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        backlog: Backlog::fixed::<1>(),
        stream: Default::default(),
        transport: Default::default(),
        egress: Default::default(),
    }
}

pub fn require_split<E>()
where
    E: Spec<Driver = DriverOnly, Timing = TimingOnly, Admission = quota::Standard>,
{
}
