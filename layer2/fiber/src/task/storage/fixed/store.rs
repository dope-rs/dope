use std::pin;

use o3::depot::arena::slab::{self, storage};

use crate::task::storage::fixed::local;

pub(super) type Inner<T, D, S> = slab::Pinned<T, D, { u32::MAX }, false, 1, S>;
pub(super) type Anchor<'a, T, D, S> = slab::Anchor<'a, T, D, { u32::MAX }, false, 1, S>;
type Member<'a, T, D, S> = slab::Anchored<'a, T, D, { u32::MAX }, false, 1, S>;

impl<T, D, S: storage::Storage> local::Member for Member<'_, T, D, S> {
    type Fiber = T;

    fn value(&mut self) -> pin::Pin<&mut T> {
        self.value()
    }

    fn discard(self) {
        self.discard();
    }
}
