use core::{fmt, hash};

use o3::collections::slab::external;

use crate::driver::route::{self, table};

type ExternalKey<Tag> = external::Key<route::Epoch, Tag>;

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

impl<Tag: route::Tag> fmt::Debug for Key<Tag> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Key")
            .field("index", &self.index())
            .field("epoch", &self.epoch())
            .finish()
    }
}

impl<Tag: route::Tag> Key<Tag> {
    pub(in crate::driver::route::table) const fn new(
        slot: route::SlotIndex,
        epoch: route::Epoch,
    ) -> Self {
        Self(table::Parts::from_components(slot, epoch))
    }

    pub(in crate::driver::route::table) fn from_external(key: ExternalKey<Tag>) -> Self {
        Self::new(
            route::SlotIndex::from_bounded(key.index()),
            key.generation(),
        )
    }

    pub(in crate::driver::route::table) fn external(self) -> ExternalKey<Tag> {
        ExternalKey::new(self.index(), self.epoch())
    }

    pub(in crate::driver::route::table) fn external_parts(
        parts: table::Parts<Tag>,
    ) -> ExternalKey<Tag> {
        ExternalKey::new(parts.index(), parts.epoch())
    }

    pub const fn index(self) -> u32 {
        self.0.slot().raw()
    }

    pub const fn slot(self) -> route::SlotIndex {
        self.0.slot()
    }

    pub const fn epoch(self) -> route::Epoch {
        self.0.epoch()
    }

    pub const fn generation(self) -> route::Epoch {
        self.epoch()
    }

    pub const fn parts(self) -> table::Parts<Tag> {
        self.0
    }
}

const _: () = {
    assert!(core::mem::size_of::<Key<route::KeyTag<1>>>() == 2 * core::mem::size_of::<u64>());
};
