use core::hash;

use o3::depot::arena::slab::external;

use crate::driver::{
    route,
    route::{indices, table},
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Key<Tag: route::Tag>(table::Parts<Tag>);

impl<Tag: route::Tag> PartialEq for Key<Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<Tag: route::Tag> Eq for Key<Tag> {}

impl<Tag: route::Tag> hash::Hash for Key<Tag> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<Tag: route::Tag> Key<Tag> {
    /// Retains the bound and generation of an identity emitted by route storage.
    pub(in crate::driver::route::table) const fn new(
        key: external::Key<route::Epoch, Tag>,
    ) -> Self {
        Self(table::Parts::new(
            indices::SlotIndex::from_field(key.index() as u64),
            key.generation(),
        ))
    }

    pub const fn slot(self) -> indices::SlotIndex {
        self.0.slot()
    }

    pub const fn epoch(self) -> route::Epoch {
        self.0.epoch()
    }

    pub const fn parts(self) -> table::Parts<Tag> {
        self.0
    }
}

const _: () = {
    assert!(core::mem::size_of::<Key<route::KeyTag<1>>>() == 2 * core::mem::size_of::<u64>());
};
