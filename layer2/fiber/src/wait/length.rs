use std::mem;

/// Number of waiters currently held by a queue.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Length(usize);

impl Length {
    pub const ZERO: Self = Self(0);

    pub(super) const fn new(waiters: usize) -> Self {
        Self(waiters)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Length>() == mem::size_of::<usize>());
