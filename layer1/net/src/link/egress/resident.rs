use crate::link::egress::{self, metadata};

/// Exact resident-byte credit independent of metadata-entry credit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Resident(u32);

impl Resident {
    pub(in crate::link::egress) const ZERO: Self = Self(0);

    pub const fn new(bytes: u32) -> Self {
        Self(bytes)
    }

    pub(in crate::link::egress) const fn into_raw(self) -> usize {
        self.0 as usize
    }

    pub const fn raw(self) -> u32 {
        self.0
    }

    pub(in crate::link::egress) const fn floor(self, lanes: metadata::LaneCount) -> Self {
        Self(self.0 / lanes.position().get())
    }

    pub(in crate::link::egress) const fn capped_add(self, added: Self) -> Self {
        Self(self.0.saturating_add(added.0))
    }

    pub(in crate::link::egress) const fn checked_add(
        self,
        added: Self,
    ) -> Result<Self, egress::Overflow<Self>> {
        match self.0.checked_add(added.0) {
            Some(total) => Ok(Self(total)),
            None => Err(egress::Overflow::new(self, added)),
        }
    }

    pub(in crate::link::egress) const fn checked_sub(
        self,
        removed: Self,
    ) -> Result<Self, egress::Deficit<Self>> {
        match self.0.checked_sub(removed.0) {
            Some(rest) => Ok(Self(rest)),
            None => Err(egress::Deficit::new(self, removed)),
        }
    }
}

impl TryFrom<usize> for Resident {
    type Error = egress::ResidentOut;

    fn try_from(bytes: usize) -> Result<Self, Self::Error> {
        if bytes > u32::MAX as usize {
            return Err(egress::ResidentOut::new(bytes));
        }
        Ok(Self(bytes as u32))
    }
}
