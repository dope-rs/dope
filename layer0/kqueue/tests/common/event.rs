use std::{cell::Cell, num::NonZero};

use dope_sys::io::reactor::complete;
use o3::{depot, depot::capacity};

#[derive(Clone, Copy)]
pub struct Capacity(pub NonZero<u32>);

// SAFETY: every copy returns its original positive bounded count.
unsafe impl depot::raw::Count for Capacity {
    fn count(self) -> capacity::Count {
        capacity::Count::new(self.0.get())
    }
}

// SAFETY: Capacity keeps the original immutable Count proof.
unsafe impl complete::raw::Capacity for Capacity {}

pub struct Event<'a> {
    pub key: Option<NonZero<u8>>,
    pub value: u8,
    pub terminal: bool,
    pub retired: &'a Cell<u8>,
}

impl complete::Event for Event<'_> {
    type Key = NonZero<u8>;

    fn token(&self) -> Option<Self::Key> {
        self.key
    }

    fn is_terminal(&self) -> bool {
        self.terminal
    }
}

impl Drop for Event<'_> {
    fn drop(&mut self) {
        self.retired.set(self.retired.get() + 1);
    }
}
