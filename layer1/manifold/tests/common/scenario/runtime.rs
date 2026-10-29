use dope_core::driver::{
    life::creation::Attempt,
    settings::{self, claim::Needs, errors},
};
use dope_manifold::timing::Throughput;
use dope_runtime::executor::{self, create};

pub struct Runtime {
    config: settings::Config,
}

impl Runtime {
    pub fn throughput() -> Self {
        Self {
            config: settings::Config::for_profile::<Throughput>(),
        }
    }

    pub fn try_for_plan<P: settings::Profile, S: settings::Resource>(
        plan: &S,
    ) -> Result<Self, errors::Claim> {
        let config = Needs::from_tree(plan)?.resolve::<P>()?;
        Ok(Self { config })
    }

    pub fn quic<const SLOTS: u16, const BYTES: u32>() -> Self {
        Self {
            config: settings::Config::new(
                settings::QueueLayout::fixed::<256, 1024>(),
                settings::Schedule::DEFAULT,
                settings::FileSlots::fixed::<0, 16>(),
                settings::Datagram::fixed::<SLOTS, BYTES>().receive(),
                settings::Domain::fixed::<2>(),
                settings::Progress::Prompt,
            ),
        }
    }

    pub const fn config(&self) -> settings::Config {
        self.config
    }

    pub fn try_executor(
        self,
        attempt: &mut Attempt,
    ) -> Result<executor::Executor<()>, create::Error<'_>> {
        executor::Executor::new(attempt, self.config)
    }
}
