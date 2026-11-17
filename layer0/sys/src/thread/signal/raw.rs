use std::fmt;

use crate::thread::signal::{AcquireFault, ReleaseFault, Signal};

#[derive(Debug)]
pub enum Acquire<R> {
    Failed(AcquireFault),
    Retained { cause: AcquireFault, receiver: R },
}

impl<R> From<AcquireFault> for Acquire<R> {
    fn from(cause: AcquireFault) -> Self {
        Self::Failed(cause)
    }
}

/// Retains prior signal policy, including partially acquired policy on failure.
pub trait Receiver: fmt::Debug + Sized {
    type Wait;
    type Source<'a>: Signal<Error = Self::Wait>
    where
        Self: 'a;

    /// # Safety
    /// Hold exclusive process SIGTERM and SIGINT authority until restored and discarded,
    /// including a receiver returned by failed acquisition.
    /// Start workers after acquisition and join them before restoration.
    unsafe fn try_new() -> Result<Self, Acquire<Self>>;
    fn source(&self) -> Self::Source<'_>;

    /// Restores the previous signal policy, retaining its owner on failure.
    fn restore(&self) -> Result<(), ReleaseFault>;
}
