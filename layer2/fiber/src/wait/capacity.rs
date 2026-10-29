use std::mem;

/// Maximum number of callers in a bounded wait queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Capacity(usize);

impl Capacity {
    pub const fn new(waiters: usize) -> Self {
        Self(waiters)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Capacity>() == mem::size_of::<usize>());
