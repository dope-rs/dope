use dope_core::driver::settings;
use dope_test::scope::Driver;

pub(crate) fn throughput() -> Driver {
    Driver::new(settings::Config::for_profile::<Profile>())
}

struct Profile;

impl settings::Profile for Profile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<4096, 65_536>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, { 64 * 1024 }>();
}
