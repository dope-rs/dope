//! Fixed indexed FIFO permits over staged admission.

use o3::alloc;

mod queue;
pub mod raw;
mod source;

pub use queue::Queue;
pub use source::Source;

/// Allocates registration storage for a backend with receive permits.
pub trait Create: Queue {
    fn try_new(capacity: usize) -> Result<Self, alloc::Error>;
}

/// Constructs an absent registration domain without allocation or failure.
pub trait Empty: Queue {
    fn new() -> Self;
}

/// The selected backend has no provided-buffer receive registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Absent;

/// The exact registration phase observed by a removal attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State<Key> {
    Gone,
    Waiting,
    Granted(Key),
    Stale,
}
