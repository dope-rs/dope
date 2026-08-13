use core::{marker, mem, num};

use o3::{collections::slab::external, num::bounded};

use crate::driver::route::{self, table};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(C)]
pub struct Token {
    epoch: num::NonZeroU64,
    meta: u64,
    marker: marker::PhantomData<*mut ()>,
}

pub trait Private {}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct SlotIndex(BoundedSlotIndex);

type BoundedSlotIndex = bounded::U32<0, { route::SLOT_MASK as u32 }>;

impl SlotIndex {
    pub const ZERO: Self = Self(BoundedSlotIndex::clamp_from_usize(0));

    pub const fn try_new(raw: u32) -> Option<Self> {
        match BoundedSlotIndex::new(raw) {
            Some(raw) => Some(Self(raw)),
            None => None,
        }
    }

    pub(crate) const fn from_bounded(raw: u32) -> Self {
        debug_assert!((raw as u64) <= route::SLOT_MASK);
        Self(BoundedSlotIndex::clamp_from_usize(raw as usize))
    }

    pub const fn raw(self) -> u32 {
        self.0.get()
    }
}

impl Default for SlotIndex {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<u16> for SlotIndex {
    fn from(raw: u16) -> Self {
        Self::from_bounded(raw as u32)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct Epoch(BoundedEpoch, marker::PhantomData<*mut ()>);

type BoundedEpoch = num::NonZeroU64;

impl Epoch {
    pub const INITIAL: Self = Self(BoundedEpoch::MIN, marker::PhantomData);

    pub const fn new(raw: u64) -> Option<Self> {
        match BoundedEpoch::new(raw) {
            Some(_) if raw > route::EPOCH_MASK => None,
            Some(epoch) => Some(Self(epoch, marker::PhantomData)),
            None => None,
        }
    }

    pub const fn raw(self) -> u64 {
        self.0.get()
    }

    pub const fn next(self) -> Option<Self> {
        match self.raw().checked_add(1) {
            Some(raw) => Self::new(raw),
            None => None,
        }
    }
}

impl From<num::NonZeroU16> for Epoch {
    fn from(raw: num::NonZeroU16) -> Self {
        Self(raw.into(), marker::PhantomData)
    }
}

impl external::Generation for Epoch {
    const INITIAL: Self = Self::INITIAL;

    fn next(self) -> Option<Self> {
        match self.raw().checked_add(1) {
            Some(raw) => Self::new(raw),
            None => None,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<SlotIndex>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Epoch>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Epoch>>() == mem::size_of::<u64>());
};

impl Token {
    pub const fn new(route: u8, slot: route::SlotIndex, epoch: route::Epoch) -> Self {
        Self::from_components(route, 0, slot, epoch.raw())
    }

    pub const fn framework(slot: route::SlotIndex) -> Self {
        Self::from_components(route::FRAMEWORK, 0, slot, 0)
    }

    pub const fn with_kind(self, kind: u8) -> Self {
        Self {
            meta: (self.meta & !(0xFF << route::KIND_SHIFT)) | ((kind as u64) << route::KIND_SHIFT),
            ..self
        }
    }

    const fn from_components(route: u8, kind: u8, slot: route::SlotIndex, epoch: u64) -> Self {
        debug_assert!(epoch <= route::EPOCH_MASK);
        Self {
            epoch: unsafe { num::NonZeroU64::new_unchecked(epoch + 1) },
            meta: ((route as u64) << route::SHIFT)
                | ((kind as u64) << route::KIND_SHIFT)
                | slot.raw() as u64,
            marker: marker::PhantomData,
        }
    }

    pub(crate) const fn try_from_framework_raw(raw: u64) -> Option<Self> {
        if raw == 0 {
            return None;
        }
        let route = (raw >> route::SHIFT) as u8;
        let kind = (raw >> route::KIND_SHIFT) as u8;
        match route::SlotIndex::try_new((raw & route::SLOT_MASK) as u32) {
            Some(slot) => Some(Self::from_components(route, kind, slot, 0)),
            None => None,
        }
    }

    pub(crate) const fn framework_raw(self) -> u64 {
        debug_assert!(self.epoch_raw() == 0);
        self.meta
    }

    pub const fn route(self) -> u8 {
        (self.meta >> route::SHIFT) as u8
    }

    pub const fn kind(self) -> u8 {
        (self.meta >> route::KIND_SHIFT) as u8
    }

    pub const fn slot(self) -> route::SlotIndex {
        route::SlotIndex::from_bounded((self.meta & route::SLOT_MASK) as u32)
    }

    pub const fn epoch(self) -> Option<route::Epoch> {
        route::Epoch::new(self.epoch_raw())
    }

    pub const fn epoch_raw(self) -> u64 {
        self.epoch.get() - 1
    }

    pub const fn same_target(self, other: Self) -> bool {
        self.epoch.get() == other.epoch.get()
            && self.route() == other.route()
            && self.slot().raw() == other.slot().raw()
    }

    pub const fn parts<Tag: route::Tag>(self) -> Option<table::Parts<Tag>> {
        if self.route() != Tag::ROUTE || (Tag::KIND != 0 && self.kind() != Tag::KIND) {
            return None;
        }
        table::Parts::new(self.slot().raw(), self.epoch_raw())
    }

    pub const fn from_key<Tag: route::Tag>(key: table::Key<Tag>) -> Self {
        Self::from_components(Tag::ROUTE, Tag::KIND, key.slot(), key.epoch().raw())
    }

    pub const fn from_target<Tag: route::Tag>(target: route::Target<'_, Tag>) -> Self {
        Self::from_parts(target.parts())
    }

    #[doc(hidden)]
    pub const fn from_parts<Tag: route::Tag>(parts: table::Parts<Tag>) -> Self {
        Self::from_components(Tag::ROUTE, Tag::KIND, parts.slot(), parts.epoch().raw())
    }
}

impl<Tag: route::Tag> From<route::Target<'_, Tag>> for Token {
    fn from(target: route::Target<'_, Tag>) -> Self {
        Self::from_target(target)
    }
}
