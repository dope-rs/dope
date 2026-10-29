use std::{marker, mem};

use o3::{base::num::bounded, depot::arena::slab::key};

/// A compact slot index and generation in one typed queue domain.
#[derive(Debug)]
#[repr(C, align(8))]
pub struct Key<Tag, const MAX: u32 = { u32::MAX }> {
    generation: key::Generation<MAX>,
    index: bounded::U32<0, { u32::MAX - 1 }>,
    tag: marker::PhantomData<*mut Tag>,
}

impl<Tag, const MAX: u32> Key<Tag, MAX> {
    pub const fn new(
        index: bounded::U32<0, { u32::MAX - 1 }>,
        epoch: key::Generation<MAX>,
    ) -> Self {
        Self {
            generation: epoch,
            index,
            tag: marker::PhantomData,
        }
    }

    pub const fn index(self) -> bounded::U32<0, { u32::MAX - 1 }> {
        self.index
    }

    pub const fn generation(self) -> key::Generation<MAX> {
        self.generation
    }

    pub(in crate::client::attempt::queue) const fn handle(self) -> key::Handle<Tag, MAX> {
        key::Handle::from_parts(key::Parts::new(self.index.get(), self.generation))
    }
}

impl<Tag, const MAX: u32> Clone for Key<Tag, MAX> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag, const MAX: u32> Copy for Key<Tag, MAX> {}

impl<Tag, const MAX: u32> PartialEq for Key<Tag, MAX> {
    fn eq(&self, other: &Self) -> bool {
        let left = u64::from(self.generation.get()) | (u64::from(self.index.get()) << 32);
        let right = u64::from(other.generation.get()) | (u64::from(other.index.get()) << 32);
        left == right
    }
}

impl<Tag, const MAX: u32> Eq for Key<Tag, MAX> {}

const _: () = {
    assert!(mem::size_of::<Key<()>>() == mem::size_of::<key::Handle>());
    assert!(mem::align_of::<Key<()>>() == mem::align_of::<key::Handle>());
    assert!(mem::size_of::<Option<Key<()>>>() == mem::size_of::<Key<()>>());
};
