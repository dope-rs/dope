use dope_core::driver::{
    schedule, settings,
    settings::{QueueLayout, Receive, Schedule},
};

pub struct Tasks;

impl settings::Profile for Tasks {
    const QUEUES: settings::QueueLayout = QueueLayout::fixed::<64, 128>();
    const SCHEDULER: settings::Schedule = Schedule::fixed::<{ schedule::LIMIT as u32 + 2 }, 0>();
    const RECEIVE: settings::Receive = Receive::fixed::<2, 4096>();
}
