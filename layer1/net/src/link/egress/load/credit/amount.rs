use std::mem::size_of;

use o3::mem;

use crate::link::egress;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(in crate::link::egress) struct Amount(u64);

impl Amount {
    const SHIFT: u32 = u32::BITS;

    pub(in crate::link::egress) const ZERO: Self = Self(0);

    pub(in crate::link::egress) const fn new(
        entries: egress::Entries,
        resident: egress::Resident,
    ) -> Self {
        Self((entries.raw() as u64) << Self::SHIFT | resident.raw() as u64)
    }

    pub(super) const fn from_raw(raw: [u32; 2]) -> Self {
        Self::new(egress::Entries::new(raw[0]), egress::Resident::new(raw[1]))
    }

    pub(in crate::link::egress) const fn entries(self) -> egress::Entries {
        egress::Entries::new((self.0 >> Self::SHIFT) as u32)
    }

    pub(in crate::link::egress) const fn resident(self) -> egress::Resident {
        egress::Resident::new(self.0 as u32)
    }

    pub(in crate::link::egress) const fn checked_sub(
        self,
        removed: Self,
    ) -> Result<Self, egress::Shortage> {
        Ok(Self::new(
            match self.entries().checked_sub(removed.entries()) {
                Ok(entries) => entries,
                Err(cause) => return Err(egress::Shortage::Entries(cause)),
            },
            match self.resident().checked_sub(removed.resident()) {
                Ok(resident) => resident,
                Err(cause) => return Err(egress::Shortage::Resident(cause)),
            },
        ))
    }

    pub(super) const fn sub(self, removed: Self) -> Self {
        Self::new(
            egress::Entries::new(self.entries().raw().wrapping_sub(removed.entries().raw())),
            egress::Resident::new(self.resident().raw().wrapping_sub(removed.resident().raw())),
        )
    }
}

const _: () = assert!(size_of::<Amount>() == size_of::<u64>());

// SAFETY: Amount stores these exact two immutable u32 components.
unsafe impl mem::raw::Amount<2, u32> for Amount {
    fn get(self) -> [u32; 2] {
        [(self.0 >> Self::SHIFT) as u32, self.0 as u32]
    }
}
