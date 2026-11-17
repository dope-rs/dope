use std::{cell::Cell, num::NonZero};

use dope::core::driver::schedule::ready::task::owner;

pub mod failure;

#[derive(Clone, Copy)]
pub struct Owner(pub u8, pub u32);

impl owner::Identity for Owner {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        (
            owner::Lane::from(NonZero::<u32>::MIN.saturating_add(u32::from(self.0))),
            owner::Generation::from_u32(self.1),
        )
    }
}

pub struct Dropped<'a>(pub &'a Cell<usize>);

impl Drop for Dropped<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}
