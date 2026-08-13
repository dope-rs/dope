use std::time;

use dope_core::driver::settings;

pub mod interval;
mod schedule;
mod timer;

pub use schedule::Schedule;

/// A strictly positive duration used for mandatory runtime bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Window(time::Duration);

impl Window {
    pub const fn new(duration: time::Duration) -> Option<Self> {
        if duration.is_zero() {
            None
        } else {
            Some(Self(duration))
        }
    }

    pub const fn from_secs(seconds: u64) -> Self {
        assert!(seconds != 0, "timing window must be positive");
        Self(time::Duration::from_secs(seconds))
    }

    pub const fn from_millis(milliseconds: u64) -> Self {
        assert!(milliseconds != 0, "timing window must be positive");
        Self(time::Duration::from_millis(milliseconds))
    }

    pub const fn get(self) -> time::Duration {
        self.0
    }
}

pub trait Policy {
    const CONNECT_DEADLINE: Window;
    const IDLE_WINDOW: Window;
    const SEND_DEADLINE: Window;
    const ABS_CONN_AGE: Window;
}

/// Balanced defaults suitable for general production services.
#[derive(Debug)]
pub struct Balanced;

impl settings::Profile for Balanced {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<8192, 65_536>();
    const MAX_ACCEPT_SLOTS: u32 = 64_511;
    const OUTBOUND_SLOTS: u32 = 1024;
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, 8192>();
}

impl Policy for Balanced {
    const CONNECT_DEADLINE: Window = Window::from_secs(5);
    const IDLE_WINDOW: Window = Window::from_secs(30);
    const SEND_DEADLINE: Window = Window::from_secs(5);
    const ABS_CONN_AGE: Window = Window::from_secs(300);
}

/// Favors low tail latency over batching and resource density.
#[derive(Debug)]
pub struct LowLatency;

impl settings::Profile for LowLatency {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<2048, 65_536>();
    const COMPLETION_PROGRESS: settings::CompletionProgress = settings::CompletionProgress::Prompt;
}

impl Policy for LowLatency {
    const CONNECT_DEADLINE: Window = Window::from_secs(2);
    const IDLE_WINDOW: Window = Window::from_secs(10);
    const SEND_DEADLINE: Window = Window::from_secs(2);
    const ABS_CONN_AGE: Window = Window::from_secs(120);
}

/// Favors sustained throughput and larger transfer batches.
#[derive(Debug)]
pub struct Throughput;

impl settings::Profile for Throughput {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<4096, 65_536>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<1024, { 64 * 1024 }>();
}

impl Policy for Throughput {
    const CONNECT_DEADLINE: Window = Window::from_secs(30);
    const IDLE_WINDOW: Window = Window::from_secs(60);
    const SEND_DEADLINE: Window = Window::from_secs(30);
    const ABS_CONN_AGE: Window = Window::from_secs(600);
}
