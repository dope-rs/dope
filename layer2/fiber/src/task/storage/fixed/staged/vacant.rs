use std::marker;

use o3::depot::arena::slab::storage;

use crate::{
    abi,
    task::storage::{
        Id,
        fixed::{direct, store},
        slots,
    },
};

pub(super) struct Vacant<'a, 'd, F: abi::Fiber<'d>, const N: usize, D, S: storage::Storage> {
    pub(super) slot: store::Vacant<'a, slots::raw::Entry<F, F::Output>, N, D, S>,
    pub(super) driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F: abi::Fiber<'d>, const N: usize, D, S: storage::Storage> direct::Insert<'d, D>
    for Vacant<'_, 'd, F, N, D, S>
{
    type Fiber = F;

    fn insert(self, fiber: F) -> Id<'d, D> {
        let (key, owner) = self.slot.insert(slots::raw::Entry::live(fiber));
        Id::from_raw(key, owner)
    }
}
