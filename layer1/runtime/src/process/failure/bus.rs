use std::mem;

use dope_core::platform::affinity;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Event {
    Installed,
    Ready,
    Drain,
    Done,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Invalid(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordSize {
    pub expected: usize,
    pub actual: usize,
}

#[derive(Debug)]
pub struct Startup {
    pub cpu: affinity::Cpu,
    pub event: Event,
    pub workers: usize,
    pub installed: usize,
    pub ready: usize,
    pub gated: bool,
}

#[derive(Debug)]
pub struct Drain {
    pub cause: DrainCause,
    pub workers: usize,
    pub drained: usize,
    pub done: usize,
}

#[derive(Debug)]
pub enum DrainCause {
    Event { cpu: affinity::Cpu, event: Event },
    Order,
}

#[derive(Debug)]
pub struct Shutdown {
    pub cpu: affinity::Cpu,
    pub event: Event,
    pub workers: usize,
    pub done: usize,
}

impl Event {
    pub(in crate::process) const fn encode(self) -> u32 {
        self as u32
    }

    pub(in crate::process) const fn decode(value: u32) -> Result<Self, Invalid> {
        match value {
            0 => Ok(Self::Installed),
            1 => Ok(Self::Ready),
            2 => Ok(Self::Drain),
            3 => Ok(Self::Done),
            4 => Ok(Self::Failed),
            value => Err(Invalid(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Write {
    Syscall(dope_core::Errno),
    Record(RecordSize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fault {
    pub event: Event,
    pub failure: Write,
    pub shutdown: Option<dope_core::Errno>,
}

#[derive(Debug)]
pub enum Report {
    Clean,
    Failed(Fault),
}

#[derive(Debug)]
pub struct Sequence {
    pub gated: bool,
    pub previous: Option<Event>,
    pub event: Event,
}

#[derive(Debug)]
pub enum Read {
    Syscall(dope_core::Errno),
    /// The coordinator consumed this Main after an exact shutdown failure.
    Stopped(dope_core::Errno),
    Closed,
    Record(RecordSize),
    Event(Invalid),
    Sequence(Sequence),
}

#[derive(Debug)]
pub enum State {
    StartupEvent(Startup),
    RuntimeEvent { cpu: affinity::Cpu, event: Event },
    DrainEvent(Drain),
    DrainOrder(Drain),
    ShutdownEvent(Shutdown),
}

const _: () = {
    assert!(mem::size_of::<Event>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Invalid>() == mem::size_of::<u32>());
};
