use dope_core::driver::settings::{self, Domain, Profile, Resource, claim::Needs, errors};
use dope_manifold::timing::Policy;
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

pub struct TimingOnly;

impl Policy for TimingOnly {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<1>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<1>();
    const SEND_DEADLINE: Positive = Positive::from_secs::<1>();
}
