pub mod fanout;
pub mod handoff;
pub mod queue;
pub(in crate::driver) mod resident;
pub mod store;
pub mod table;
mod waker;

pub use waker::Waker;

/// Position observed from a lane or slot already validated by its registry.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Position(usize);

impl Position {
    pub(in crate::driver::schedule::ready) const fn new(index: usize) -> Self {
        Self(index)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Position>() == std::mem::size_of::<usize>());
    assert!(std::mem::align_of::<Position>() == std::mem::align_of::<usize>());
};

/// Progress observed after a wake drain attempt.
///
/// Wakes are delivered before return; pending work stays with the registry.
/// Scheduling may use this result or the registry's pending-work state.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Drain {
    Empty,
    Woke,
    Exhausted,
}

/// Whether a wake was delivered; the wake effect is complete before return.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Once {
    Empty,
    Woke,
}
