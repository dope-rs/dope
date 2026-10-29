use dope_core::driver::settings;
use o3::base::time::Positive;

pub mod interval;
mod schedule;
pub(crate) mod timer;
pub mod watchdog;

pub use schedule::Schedule;

pub trait Policy {
    const CONNECT_DEADLINE: Positive;
    const IDLE_WINDOW: Positive;
    const SEND_DEADLINE: Positive;
}

/// Balanced defaults suitable for general production services.
pub struct Balanced;

impl settings::Profile for Balanced {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<8192, 65_536>();
    const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 1024>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, 8192>();
}

impl Policy for Balanced {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<5>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<30>();
    const SEND_DEADLINE: Positive = Positive::from_secs::<5>();
}

/// Favors sustained throughput and larger transfer batches.
pub struct Throughput;

impl settings::Profile for Throughput {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<4096, 65_536>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, { 64 * 1024 }>();
}

impl Policy for Throughput {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<30>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<60>();
    const SEND_DEADLINE: Positive = Positive::from_secs::<30>();
}
