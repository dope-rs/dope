use std::fmt;

use crate::thread::signal::{AcquireFault, ReleaseFault, Signal};

/// Retains prior signal policy; failed acquisition leaves that policy unchanged.
pub trait Receiver: fmt::Debug + Sized {
    type Acquire: Into<AcquireFault>;
    type Wait;
    type Source<'a>: Signal<Error = Self::Wait>
    where
        Self: 'a;

    /// # Safety
    /// Hold exclusive process SIGTERM authority until successfully restored and discarded.
    /// Start workers after acquisition and join them before restoration.
    unsafe fn try_new() -> Result<Self, Self::Acquire>;
    fn source(&self) -> Self::Source<'_>;

    /// Restores the previous signal policy, retaining its owner on failure.
    fn restore(&self) -> Result<(), ReleaseFault>;
}
