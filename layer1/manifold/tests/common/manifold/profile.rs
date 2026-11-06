use dope_core::driver::settings::{self, Domain, Profile, Resource, claim::Needs, errors};

pub struct DriverOnly;

impl Profile for DriverOnly {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
}

impl DriverOnly {
    pub fn try_domains(tree: impl Resource) -> Result<Domain, errors::Claim> {
        Ok(Needs::from_tree(&tree)?.resolve::<Self>()?.domains())
    }
}
