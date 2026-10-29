use std::mem;

use crate::driver::schedule::clock::timer::heap::{
    Min,
    min::{Lease, core},
};

/// Exclusive proof that one resident slot is currently available.
pub struct Vacancy<'a, 'store, 'id, P: Ord, R, Tag = ()> {
    pub(super) heap: &'a mut Min<'store, 'id, P, R, Tag>,
    pub(super) free: core::Free,
}

impl<'a, 'store, 'id, P: Ord, R, Tag> Vacancy<'a, 'store, 'id, P, R, Tag> {
    /// Leases the proved vacant resident slot in constant time.
    pub fn lease(self, item: R) -> Lease<'id, Tag> {
        let live = self.heap.core.lease(item, self.free);
        Lease::from_live(live)
    }
}

const _: () = {
    assert!(mem::size_of::<Vacancy<'_, '_, '_, u8, u8>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Vacancy<'_, '_, '_, u8, u8>>>() == mem::size_of::<usize>());
};
