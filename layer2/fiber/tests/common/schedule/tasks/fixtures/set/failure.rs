use dope::core::driver::{
    settings,
    settings::{FileSlots, QueueLayout, Receive, Schedule},
};

pub struct Tight;

impl settings::Profile for Tight {
    const QUEUES: settings::QueueLayout = QueueLayout::fixed::<8, 8>();
    const SCHEDULER: settings::Schedule = Schedule::fixed::<3, 1>();
    const FILES: settings::FileSlots = FileSlots::fixed::<0, 1>();
    const RECEIVE: settings::Receive = Receive::fixed::<8, 64>();
}
