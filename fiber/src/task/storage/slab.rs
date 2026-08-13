use std::marker;

use o3::collections::{
    self,
    slab::{self, pinned},
};

use crate::{abi, task::storage};

pub struct Slab<'d, F, Tag = ()>
where
    F: abi::Fiber<'d>,
{
    inner: pinned::Pool<F, Tag>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F, Tag> Slab<'d, F, Tag>
where
    F: abi::Fiber<'d>,
{
    pub fn try_with_capacity(
        capacity: slab::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            inner: pinned::Pool::try_with_capacity(capacity)?,
            driver: marker::PhantomData,
        })
    }

    pub fn insert(&mut self, fiber: F) -> Option<storage::Id<'d, Tag>> {
        self.inner.insert(fiber).ok().map(storage::Id::from_key)
    }

    pub fn remove(&mut self, id: storage::Id<'d, Tag>) -> bool {
        self.inner.remove_parts(id.parts())
    }
}
