use std::mem;

use o3::depot::{
    self,
    index::{self, Stored},
};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Id(u16);

impl Id {
    pub(crate) const fn from_bytes(id: [u8; 2]) -> Self {
        Self(u16::from_be_bytes(id))
    }

    pub(crate) const fn from_wire(id: u16) -> Self {
        Self(id)
    }

    pub(crate) const fn from_ordinal(index: index::Ordinal<Self, u16>) -> Self {
        Self(index.raw())
    }

    pub(crate) const fn wrapping_add(self, offset: u16) -> Self {
        Self(self.0.wrapping_add(offset))
    }

    pub(crate) const fn wire(self) -> u16 {
        self.0
    }

    pub(crate) const fn ordinal(self) -> index::Ordinal<Self, u16> {
        index::Ordinal::new(self.0)
    }
}

const _: () = {
    assert!(Id::from_wire(u16::MIN).wire() == u16::MIN);
    assert!(Id::from_wire(u16::MAX).wire() == u16::MAX);
};

// SAFETY: `index` is the stored wire number, so copies and equal identities keep
// the same direct-address position. `VACANT` is outside every stored identity
// (`into_raw` never exceeds `u16::MAX`), live identities and `index` round-trip
// exactly, and `decode` rejects exactly `VACANT` and any out-of-domain raw.
unsafe impl depot::raw::Key for Id {
    type Raw = u32;
    type Live = Id;

    const VACANT: Self::Raw = u32::MAX;

    fn index(self) -> usize {
        usize::from(self.0)
    }

    fn into_raw(self) -> Self::Raw {
        u32::from(self.0)
    }

    fn from_raw(raw: Stored<Self>) -> Self {
        Self(raw.into_raw() as u16)
    }

    fn decode(raw: Self::Raw) -> Option<Self> {
        u16::try_from(raw).ok().map(Self)
    }

    fn into_live(self) -> Self::Live {
        self
    }

    fn from_live(live: Self::Live) -> Self {
        live
    }
}

const _: () = assert!(mem::size_of::<Id>() == mem::size_of::<u16>());
