use dope_core::driver::settings::{CompletionProgress, Config, Profile};
use dope_manifold::{
    Bundle, Env,
    listener::config::StandardAdmission,
    timing::{Balanced, LowLatency, Policy, Throughput, Window},
};
use dope_net::{tcp::Tcp, wire::Identity};

struct DriverOnly;

impl Profile for DriverOnly {
    const QUEUES: dope_core::driver::settings::QueueLayout =
        dope_core::driver::settings::QueueLayout::fixed::<64, 128>();
}

struct TimingOnly;

impl Policy for TimingOnly {
    const CONNECT_DEADLINE: Window = Window::from_secs(1);
    const IDLE_WINDOW: Window = Window::from_secs(1);
    const SEND_DEADLINE: Window = Window::from_secs(1);
    const ABS_CONN_AGE: Window = Window::from_secs(1);
}

fn require_split<E>()
where
    E: Env<Driver = DriverOnly, Timing = TimingOnly, Admission = StandardAdmission>,
{
}

#[test]
fn balanced_profile_bounds_connection_age() {
    assert!(!Balanced::CONNECT_DEADLINE.get().is_zero());
    assert!(!Balanced::ABS_CONN_AGE.get().is_zero());
    assert!(!Balanced::SEND_DEADLINE.get().is_zero());
}

#[test]
fn low_latency_tcp_profile_preserves_its_receive_ceiling() {
    let config = Config::for_tcp_profile::<LowLatency>(1).expect("low-latency profile");
    assert_eq!(config.receive(), LowLatency::RECEIVE);
    assert_eq!(config.receive().entries(), 4096);
}

#[test]
fn driver_profiles_preserve_completion_progress_intent() {
    assert_eq!(
        Config::for_tcp_profile::<LowLatency>(1)
            .expect("low-latency profile")
            .completion_progress(),
        CompletionProgress::Prompt
    );
    for progress in [
        Config::for_tcp_profile::<Balanced>(1)
            .expect("balanced profile")
            .completion_progress(),
        Config::for_tcp_profile::<Throughput>(1)
            .expect("throughput profile")
            .completion_progress(),
    ] {
        assert_eq!(progress, CompletionProgress::BatchedWhenSupported);
    }
}

#[test]
fn bundle_separates_driver_and_timing_types() {
    type Split = Bundle<Tcp, Identity, DriverOnly, TimingOnly>;
    require_split::<Split>();
    assert_eq!(std::mem::size_of::<Split>(), 0);
}
