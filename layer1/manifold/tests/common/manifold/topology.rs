use dope_core::driver::{
    route::KeyTag,
    settings,
    settings::{claim::Needs, errors},
};
use dope_manifold::network::client::topology::plan;

pub struct Topology;

impl Topology {
    pub fn try_domains<A: plan::Input>() -> Result<settings::Domain, errors::Claim> {
        let layout = plan::Layout::<KeyTag<7>, 1, plan::Standard, A>::new(1)?;
        Ok(Needs::from_tree(&layout)?.resolve::<Self>()?.domains())
    }
}

impl settings::Profile for Topology {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<8, 8>();
    const SCHEDULER: settings::Schedule = settings::Schedule::fixed::<0, 0>();
    const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 1>();
}
