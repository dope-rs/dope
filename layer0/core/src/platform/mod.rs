/// Thread-local CPU-affinity capabilities.
pub mod affinity;

/// Process-signal acquisition and scoped waiting.
pub mod signal;

/// Borrowed descriptor readiness with owned poll storage.
pub mod poll;

/// Owned process-control record channels.
pub mod channel;

/// Pipe notification endpoints and timed observation.
pub mod wake;

/// Failed read or update of descriptor flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flags {
    Get(crate::Errno),
    Set(crate::Errno),
}

impl Flags {
    fn cloexec(error: dope_sys::Cloexec) -> Self {
        match error {
            dope_sys::Cloexec::Get(error) => Self::Get(error),
            dope_sys::Cloexec::Set(error) => Self::Set(error),
        }
    }
    fn nonblock(error: dope_sys::Nonblock) -> Self {
        match error {
            dope_sys::Nonblock::Get(error) => Self::Get(error),
            dope_sys::Nonblock::Set(error) => Self::Set(error),
        }
    }
}

/// Scoped ownership of worker inputs, native threads, and joined outputs.
pub mod thread;
