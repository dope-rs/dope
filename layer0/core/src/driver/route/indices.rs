use std::{mem, num};

use o3::base::num::bounded;

use crate::driver::{route, route::table::conn};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SlotIndex(pub(super) bounded::NonZeroU32<1, { route::SLOT_MASK as u32 + 1 }>);

impl From<SlotIndex> for num::NonZero<u32> {
    fn from(slot: SlotIndex) -> Self {
        slot.stored()
    }
}

/// Exact raw position rejected by the route slot ABI bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct SlotOverflow(pub(super) usize);

/// The first position after a nonempty route segment.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Edge(pub(super) SlotIndex);

impl SlotIndex {
    pub const ZERO: Self = Self(bounded::NonZeroU32::exact::<1>());
    pub const MAX: Self = Self(bounded::NonZeroU32::exact::<{ route::SLOT_MASK as u32 + 1 }>());

    pub const fn fixed<const RAW: u32>() -> Self {
        const { assert!(RAW <= route::SLOT_MASK as u32, "slot bound") };
        // SAFETY: the const bound keeps the one-based encoding of RAW in 1..=SLOT_MASK + 1.
        unsafe { Self::encode(RAW) }
    }

    pub const fn index(self) -> bounded::U32<0, { route::SLOT_MASK as u32 }> {
        self.0.into_index()
    }

    pub(crate) const fn into_le24(self) -> [u8; 3] {
        let bytes = self.raw().to_le_bytes();
        [bytes[0], bytes[1], bytes[2]]
    }

    /// One-based position encoding for collections reserving zero as empty.
    pub const fn position(self) -> bounded::U32<1, { route::SLOT_MASK as u32 + 1 }> {
        self.0.into_u32()
    }

    /// Decodes one packed slot field; bits outside the mask belong to other fields.
    pub(crate) const fn from_field(bits: u64) -> Self {
        // SAFETY: the mask keeps the one-based encoding within 1..=SLOT_MASK + 1.
        unsafe { Self::encode((bits & route::SLOT_MASK) as u32) }
    }

    /// Admits one raw position; out-of-range positions are rejected, never truncated.
    pub(crate) const fn from_raw(raw: u64) -> Option<Self> {
        if raw > route::SLOT_MASK {
            return None;
        }
        // SAFETY: the guard proves the raw position within SLOT_MASK.
        Some(unsafe { Self::encode(raw as u32) })
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0.get() - 1
    }

    pub(in crate::driver) const fn stored(self) -> num::NonZero<u32> {
        self.0.nonzero()
    }

    /// Encodes one raw position the caller proved within `SLOT_MASK`.
    ///
    /// # Safety
    /// `raw` must not exceed `route::SLOT_MASK`.
    const unsafe fn encode(raw: u32) -> Self {
        // SAFETY: the caller admits raw <= SLOT_MASK, so the one-based encoding stays in range.
        Self(
            unsafe {
                bounded::raw::NonZeroProof::assume(num::NonZero::<u32>::MIN.saturating_add(raw))
            }
            .retain(),
        )
    }

    fn try_raw(raw: u64, actual: usize) -> Result<Self, SlotOverflow> {
        Self::from_raw(raw).ok_or_else(|| SlotOverflow::new(actual))
    }
}

impl Edge {
    pub const fn checked_add(self, local: SlotIndex) -> Option<SlotIndex> {
        SlotIndex::from_raw(self.raw() as u64 + local.raw() as u64)
    }

    /// Both raw positions retain 0..=SLOT_MASK; after the lower-bound guard,
    /// 0 <= slot - base <= slot <= SLOT_MASK.
    pub const fn offset(self, slot: SlotIndex) -> Option<SlotIndex> {
        if slot.raw() < self.raw() {
            None
        } else {
            Some(SlotIndex::from_field((slot.raw() - self.raw()) as u64))
        }
    }

    pub const fn slot(self) -> SlotIndex {
        self.0
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0.raw()
    }
}

impl From<conn::Capacity> for Edge {
    fn from(capacity: conn::Capacity) -> Self {
        Self(SlotIndex::from_field(u64::from(capacity.raw())))
    }
}

impl TryFrom<u32> for SlotIndex {
    type Error = SlotOverflow;

    fn try_from(raw: u32) -> Result<Self, Self::Error> {
        Self::try_raw(u64::from(raw), raw as usize)
    }
}

impl TryFrom<usize> for SlotIndex {
    type Error = SlotOverflow;

    fn try_from(raw: usize) -> Result<Self, Self::Error> {
        Self::try_raw(raw as u64, raw)
    }
}

impl Default for SlotIndex {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<u16> for SlotIndex {
    fn from(raw: u16) -> Self {
        const { assert!(route::SLOT_MASK >= u16::MAX as u64, "slot bound") };
        // SAFETY: the const bound admits every u16 position.
        unsafe { Self::encode(raw as u32) }
    }
}

impl SlotOverflow {
    pub(crate) const fn new(actual: usize) -> Self {
        Self(actual)
    }

    pub const fn actual(self) -> usize {
        self.0
    }
}

const _: () = {
    const _: () = assert!(mem::size_of::<Option<Edge>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<SlotIndex>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<SlotIndex>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Edge>() == mem::size_of::<u32>());
};

impl From<bounded::U32<0, { route::SLOT_MASK as u32 }>> for SlotIndex {
    fn from(index: bounded::U32<0, { route::SLOT_MASK as u32 }>) -> Self {
        // SAFETY: the source bound admits every raw position.
        unsafe { Self::encode(index.get()) }
    }
}
