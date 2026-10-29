use dope_core::driver::settings;

pub(super) struct Throughput;

impl settings::Profile for Throughput {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<4096, 65_536>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, { 64 * 1024 }>();
}

pub struct Files;

impl settings::Profile for Files {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
    const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 16>();
}

pub struct Receive;

impl settings::Profile for Receive {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
    const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 16>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<4096, 8192>();
}

pub struct Domain;

impl settings::Profile for Domain {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
}

pub struct Minimal;

impl settings::Profile for Minimal {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<8, 8>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<2, 1>();
    const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 1>();
    const SCHEDULER: settings::Schedule = settings::Schedule::fixed::<0, 0>();
}

pub struct StreamOnly;

impl settings::Profile for StreamOnly {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<2, 1>();
}

pub struct Prompt;

impl settings::Profile for Prompt {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
    const COMPLETION_PROGRESS: settings::Progress = settings::Progress::Prompt;
}

pub struct Batched;

impl settings::Profile for Batched {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
}
