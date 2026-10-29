use std::marker;

use o3::depot;

use crate::driver::receive::credit::{self, storage};

// SAFETY: the exact active storage is borrowed through the returned slot, so
// its group cannot retire while the admitted logical slot is in use.
unsafe impl<'a, 'd> depot::raw::Project<usize> for &'a credit::Storage<'d> {
    type Output = credit::Slot<'a, 'd>;

    unsafe fn project(self, index: usize) -> Self::Output {
        // SAFETY: the caller proves index is initialized in this exact active
        // storage's logical extent, not merely inside its rounded backing.
        let entry = unsafe { self.entries.active.entries.get_unchecked(index) };
        credit::Slot {
            entry: storage::Slot {
                entry,
                active: marker::PhantomData,
            },
            driver: self.driver,
        }
    }
}
