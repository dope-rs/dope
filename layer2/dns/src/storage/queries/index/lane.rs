use std::mem;

use o3::depot::index;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::storage) struct Lane(u16);

impl Lane {
    pub(in crate::storage::queries) const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    pub(in crate::storage::queries) const fn from_ordinal(
        index: index::Ordinal<Self, u16>,
    ) -> Self {
        Self(index.raw())
    }

    pub(in crate::storage::queries::index) const fn raw(self) -> u16 {
        self.0
    }

    pub(in crate::storage::queries) const fn ordinal(self) -> index::Ordinal<Self, u16> {
        index::Ordinal::new(self.0)
    }
}

impl From<Lane> for usize {
    fn from(lane: Lane) -> Self {
        Self::from(lane.0)
    }
}

const _: () = assert!(mem::size_of::<Lane>() == mem::size_of::<u16>());
