use o3::depot::arena::slab::external;

use crate::driver::{route, route::table};

#[repr(transparent)]
pub struct Entry<'a, T, Tag: route::Tag> {
    pub(in crate::driver::route::table) inner:
        external::VacantSlot<'a, T, route::Epoch, Tag, { table::PARTITIONS }>,
}

impl<'a, T, Tag: route::Tag> Entry<'a, T, Tag> {
    pub fn insert(self, value: T) {
        self.inner.commit(value);
    }

    pub fn stage(self, value: T) -> Staged<'a, T, Tag> {
        Staged {
            inner: self.inner.stage(value),
        }
    }

    pub fn key(&self) -> table::Key<Tag> {
        table::Key::new(self.inner.key())
    }
}

#[repr(transparent)]
pub struct Staged<'a, T, Tag: route::Tag> {
    inner: external::StagedSlot<'a, T, route::Epoch, Tag, { table::PARTITIONS }>,
}

impl<'a, T, Tag: route::Tag> Staged<'a, T, Tag> {
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn publish(self) {
        self.inner.publish();
    }

    pub fn reject(self) -> T {
        self.inner.reject()
    }
}
