use crate::memory;

/// Current cleanup evidence is independent of the original construction error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure<S, W> {
    Storage(S),
    Memory(memory::Error),
    Wait(W),
}
