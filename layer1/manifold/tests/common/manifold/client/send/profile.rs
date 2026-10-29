use dope_core::driver::settings;
use dope_manifold::timing::Policy;
use o3::base::time::Positive;

pub struct Fast;

impl settings::Profile for Fast {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
}

impl Policy for Fast {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<2>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<2>();
    const SEND_DEADLINE: Positive = Positive::from_millis::<100>();
}

pub struct Slow;

impl settings::Profile for Slow {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
}

impl Policy for Slow {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<2>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<2>();
    const SEND_DEADLINE: Positive = Positive::from_secs::<2>();
}
