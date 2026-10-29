use std::mem;

/// Identity slot selected within a dense wait table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Index(usize);

impl Index {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Index>() == mem::size_of::<usize>());
