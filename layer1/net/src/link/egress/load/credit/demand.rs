use o3::base::num::bounded;

use crate::link::egress::{self, load::credit};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(in crate::link::egress) struct Demand<const MAX: u32 = { u32::MAX }>(credit::Amount);

impl<const MAX: u32> Demand<MAX> {
    pub(super) const ZERO: Self = Self(credit::Amount::ZERO);

    pub(super) const fn entries(self) -> egress::Entries {
        self.0.entries()
    }

    pub(super) const fn resident(self) -> egress::Resident {
        self.0.resident()
    }

    pub(super) fn fits(self, free: credit::Amount) -> bool {
        self.entries() <= free.entries() && self.resident() <= free.resident()
    }
}

impl Demand {
    pub(super) const fn from_raw(raw: [u32; 2]) -> Self {
        Self(credit::Amount::from_raw(raw))
    }
}

impl Demand<2> {
    /// Saturating protected-credit subtraction preserves Queued's 0..=2 bound.
    pub(super) fn queued(account: credit::Account<'_, '_>, amount: credit::Queued) -> Self {
        Self(account.demand(amount.amount()).0)
    }

    pub(super) const fn entry_bound(self) -> bounded::U32<0, 2> {
        // SAFETY: o3 Lane::demand subtracts protected credit with per-component
        // saturation, so each demand is <= the original Queued request (<= 2
        // entries). ZERO is the only other constructor of Demand<2>.
        unsafe { bounded::raw::Proof::assume(self.entries().raw()) }.retain()
    }
}

const _: () = {
    assert!(std::mem::size_of::<Demand>() == std::mem::size_of::<u64>());
    assert!(std::mem::size_of::<Demand<2>>() == std::mem::size_of::<u64>());
};
