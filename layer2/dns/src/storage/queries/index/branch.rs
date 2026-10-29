use std::{mem, num};

use o3::depot::index;

use crate::{query::family, storage::queries::index::Lane};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::storage) struct Branch(num::NonZero<u16>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Retirement<const LANES: usize>(u16);

impl Branch {
    pub(in crate::storage) const fn new(lane: Lane, family: family::Version) -> Self {
        Self::from_ordinal(index::Ordinal::new((lane.raw() << 1) | family as u16))
    }

    /// Layout and retirement keep ordinals below u16::MAX, so encoding cannot saturate.
    pub(in crate::storage::queries) const fn from_ordinal(
        index: index::Ordinal<Self, u16>,
    ) -> Self {
        Self(num::NonZero::<u16>::MIN.saturating_add(index.raw()))
    }

    pub(in crate::storage) const fn lane(self) -> Lane {
        Lane::from_raw(self.raw() >> 1)
    }

    pub(in crate::storage) const fn family(self) -> family::Version {
        family::Version::from_bit(self.raw() & 1)
    }

    pub(in crate::storage::queries) const fn raw(self) -> u16 {
        self.0.get() - 1
    }

    pub(in crate::storage::queries) const fn ordinal(self) -> index::Ordinal<Self, u16> {
        index::Ordinal::new(self.raw())
    }
}

impl<const LANES: usize> Retirement<LANES> {
    const VALID: () = {
        assert!(LANES != 0);
        assert!(LANES <= (u16::MAX as usize - 1) / family::Version::COUNT);
    };

    const LIMIT: u16 = {
        let () = Self::VALID;
        (LANES * family::Version::COUNT) as u16
    };

    pub(crate) const fn new() -> Self {
        let () = Self::VALID;
        Self(0)
    }

    pub(in crate::storage) const fn next(self) -> Option<(Branch, Self)> {
        if self.0 == Self::LIMIT {
            return None;
        }
        let branch = Branch::from_ordinal(index::Ordinal::new(self.0));
        Some((branch, Self(self.0 + 1)))
    }
}

const _: () = assert!(mem::size_of::<Branch>() == mem::size_of::<u16>());
const _: () = assert!(mem::size_of::<Option<Branch>>() == mem::size_of::<u16>());
const _: () = assert!(mem::size_of::<Retirement<1>>() == mem::size_of::<u16>());
