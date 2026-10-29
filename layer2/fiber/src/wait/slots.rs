use std::mem;

/// Number of identity slots in a dense wait table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Slots(usize);

impl Slots {
    pub const fn new(slots: usize) -> Self {
        Self(slots)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Slots>() == mem::size_of::<usize>());
