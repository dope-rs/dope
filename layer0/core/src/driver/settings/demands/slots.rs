use o3::depot::capacity;

use crate::driver::{route::table::conn, schedule::ready::task::owner, settings::demands};

/// Scheduler-owned index capacity matching ready and timer slot indices.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct Slots(u32);

/// Exact operands whose scheduler-slot sum is not representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sum {
    current: Slots,
    added: Slots,
}

impl Sum {
    pub const fn parts(&self) -> (&Slots, &Slots) {
        (&self.current, &self.added)
    }
}

impl Slots {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u32::MAX);

    pub const fn fixed<const SLOTS: u32>() -> Self {
        Self(SLOTS)
    }

    #[must_use]
    pub const fn count(self) -> capacity::Count {
        capacity::Count::new(self.0)
    }

    pub const fn max(self, next: Self) -> Self {
        if self.0 >= next.0 { self } else { next }
    }

    pub const fn checked_add(self, added: Self) -> Result<Self, Sum> {
        match self.0.checked_add(added.0) {
            Some(total) => Ok(Self(total)),
            None => Err(Sum {
                current: self,
                added,
            }),
        }
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0
    }
}

impl TryFrom<usize> for Slots {
    type Error = demands::Overflow;

    fn try_from(slots: usize) -> Result<Self, Self::Error> {
        Ok(Self(capacity::Count::try_from(slots)?.raw()))
    }
}

impl From<conn::Capacity> for Slots {
    fn from(capacity: conn::Capacity) -> Self {
        Self(capacity.count().get())
    }
}

impl From<owner::Capacity> for Slots {
    fn from(capacity: owner::Capacity) -> Self {
        Self(capacity.count().get())
    }
}
