use dope_core::driver::{life::creation::Attempt, settings};
use dope_runtime::executor::{self, create};

pub struct Configured(settings::Config);

impl Configured {
    pub fn throughput() -> Self {
        Self(settings::Config::for_profile::<Profile>())
    }

    pub fn quic<const SLOTS: u16, const BYTES: u32>() -> Self {
        Self(settings::Config::new(
            settings::QueueLayout::fixed::<256, 1024>(),
            settings::Schedule::DEFAULT,
            settings::FileSlots::fixed::<0, 16>(),
            settings::Datagram::fixed::<SLOTS, BYTES>().receive(),
            settings::Domain::fixed::<2>(),
            settings::Progress::Prompt,
        ))
    }

    pub const fn config(&self) -> settings::Config {
        self.0
    }

    pub fn try_executor(
        self,
        attempt: &mut Attempt,
    ) -> Result<executor::Executor, create::Error<'_>> {
        executor::Executor::new(attempt, self.0)
    }

    pub fn try_source<B: executor::Build>(
        self,
        attempt: &mut Attempt,
        source: B,
    ) -> Result<executor::Executor<(), B::Shutdown>, create::Error<'_>> {
        source.try_build(attempt, self.0)
    }
}

struct Profile;

impl settings::Profile for Profile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<4096, 65_536>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, { 64 * 1024 }>();
}
