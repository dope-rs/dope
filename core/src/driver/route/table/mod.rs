mod capacity;
mod cell;
mod connection;
pub mod entries;
mod key;
mod parts;
mod slab;

use o3::collections::slab::external;

const PARTITIONS: usize = 2;

type ExternalSlab<T, Tag> = external::Exclusive<T, super::Epoch, Tag, { u32::MAX }, PARTITIONS>;
type ExternalEntries<'a, T, Tag> =
    external::Entries<'a, T, super::Epoch, Tag, { u32::MAX }, PARTITIONS>;
type ExternalEntriesMut<'a, T, Tag> =
    external::EntriesMut<'a, T, super::Epoch, Tag, { u32::MAX }, PARTITIONS>;
type ExternalOccupiedEntry<'a, T, Tag> =
    external::OccupiedEntry<'a, T, super::Epoch, Tag, { u32::MAX }, PARTITIONS>;
type ExternalVacantEntry<'a, T, Tag> =
    external::VacantEntry<'a, T, super::Epoch, Tag, { u32::MAX }, PARTITIONS>;

pub use capacity::Capacity;
pub use cell::slab::CellSlab;
pub use connection::capacity::ConnectionCapacity;
pub use key::Key;
pub use parts::Parts;
pub use slab::Slab;

pub struct Entries<'a, T, Tag: super::Tag>(ExternalEntries<'a, T, Tag>);

impl<'a, T, Tag: super::Tag> Entries<'a, T, Tag> {
    fn new(inner: ExternalEntries<'a, T, Tag>) -> Self {
        Self(inner)
    }

    pub fn at_parts(self, parts: Parts<Tag>) -> Option<&'a T> {
        self.0.get(Key::external_parts(parts))
    }

    pub fn current(self, slot: super::SlotIndex) -> Option<(&'a T, Key<Tag>)> {
        self.0
            .current(slot.raw())
            .map(|(value, key)| (value, Key::from_external(key)))
    }

    pub fn values(self) -> impl Iterator<Item = &'a T> {
        self.0.values()
    }
}

pub struct Mut<'a, T, Tag: super::Tag>(ExternalEntriesMut<'a, T, Tag>);

impl<'a, T, Tag: super::Tag> Mut<'a, T, Tag> {
    fn new(inner: ExternalEntriesMut<'a, T, Tag>) -> Self {
        Self(inner)
    }

    pub fn at_parts(self, parts: Parts<Tag>) -> Option<&'a mut T> {
        self.0.get(Key::external_parts(parts))
    }

    pub fn values(self) -> impl Iterator<Item = &'a mut T> {
        self.0.values()
    }
}
