use std::mem;

use o3::depot::index;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Id(u16);

impl Id {
    pub(crate) const CAPACITY: usize = u16::MAX as usize + 1;

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

const _: () = assert!(mem::size_of::<Id>() == mem::size_of::<u16>());
