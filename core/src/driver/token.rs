use core::marker::PhantomData;
use core::num::{NonZeroU32, NonZeroU64};

mod slab;

pub use slab::{Key, KeyParts, TokenCapacity, TokenCellSlab, TokenSlab, TokenSlabVacantEntry};

pub const ROUTE_FRAMEWORK: u8 = 255;

pub const ROUTE_SHIFT: u32 = 56;
pub const KIND_SHIFT: u32 = 48;
const EPOCH_SHIFT: u32 = 24;
const ROUTE_MASK: u64 = 0xFF << ROUTE_SHIFT;
const KIND_MASK: u64 = 0xFF << KIND_SHIFT;
const TARGET_MASK: u64 = !KIND_MASK;
pub const SLOT_BITS: u32 = 24;
const EPOCH_BITS: u32 = 24;
pub const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
pub const EPOCH_MASK: u64 = (1 << EPOCH_BITS) - 1;

pub struct KeyTag<const ROUTE: u8, const KIND: u8 = 0>;

pub trait TokenTag {
    const ROUTE: u8;
    const KIND: u8;
    const MASK: u64;
    const VALUE: u64;
}

impl<const ROUTE: u8, const KIND: u8> TokenTag for KeyTag<ROUTE, KIND> {
    const ROUTE: u8 = ROUTE;
    const KIND: u8 = KIND;
    const MASK: u64 = ROUTE_MASK | if KIND == 0 { 0 } else { KIND_MASK };
    const VALUE: u64 = (ROUTE as u64) << ROUTE_SHIFT | (KIND as u64) << KIND_SHIFT;
}

pub mod kind {
    pub const ACCEPT: u8 = 1;
    pub const RECV: u8 = 2;
    pub const SEND: u8 = 3;
    pub const TIMER: u8 = 4;
    pub const SOCKET: u8 = 5;
    pub const CONNECT: u8 = 6;
    pub const SHUTDOWN: u8 = 7;
    pub const SETSOCKOPT: u8 = 8;
    pub const WRITE: u8 = 9;
    pub const SYNC: u8 = 10;
    pub const OPEN: u8 = 11;
    pub const READ: u8 = 12;
    pub const RECV_DISCARD: u8 = 14;
    pub const CREATE: u8 = 15;
    pub const CLOSE_PREP: u8 = 16;
    pub const CLOSE: u8 = 17;
    pub const STAT: u8 = 18;
    pub const ONE_SHOT: u8 = 19;
    pub const TASK_QUEUE: u8 = 21;
    pub const RECV_CREDIT_HELD: u8 = 22;
    pub const RECV_CREDIT_RELEASED: u8 = 23;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Default)]
#[repr(transparent)]
pub struct SlotIndex(u32);

impl SlotIndex {
    pub const ZERO: Self = Self(0);

    pub const fn try_new(raw: u32) -> Option<Self> {
        if (raw as u64) <= SLOT_MASK {
            Some(Self::from_bounded(raw))
        } else {
            None
        }
    }

    pub(crate) const fn from_bounded(raw: u32) -> Self {
        debug_assert!((raw as u64) <= SLOT_MASK);
        Self(raw)
    }

    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl From<u16> for SlotIndex {
    fn from(raw: u16) -> Self {
        Self(raw as u32)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct Epoch(NonZeroU32);

impl Epoch {
    pub const INITIAL: Self = Self(NonZeroU32::MIN);
    pub const MAX: Self = Self(NonZeroU32::MIN.saturating_add(EPOCH_MASK as u32 - 1));

    pub const fn new(raw: u32) -> Option<Self> {
        match NonZeroU32::new(raw) {
            Some(raw) if raw.get() <= EPOCH_MASK as u32 => Some(Self(raw)),
            Some(_) | None => None,
        }
    }

    pub const fn raw(self) -> u32 {
        self.0.get()
    }

    pub const fn next(self) -> Option<Self> {
        if self.0.get() < EPOCH_MASK as u32 {
            Some(Self(self.0.saturating_add(1)))
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct Token(NonZeroU64, PhantomData<*mut ()>);

impl Token {
    pub const fn new(route: u8, slot: SlotIndex, epoch: Epoch) -> Self {
        let raw = Self::pack(route, slot, epoch.raw());
        // `Epoch` proves that the target bits are nonzero.
        Self::from_proven_target(raw)
    }

    pub const fn framework(slot: SlotIndex) -> Self {
        let raw = Self::pack(ROUTE_FRAMEWORK, slot, 0);
        // `ROUTE_FRAMEWORK` proves that the target bits are nonzero.
        Self::from_proven_target(raw)
    }

    pub const fn with_kind(self, kind: u8) -> Self {
        let raw = (self.0.get() & !KIND_MASK) | ((kind as u64) << KIND_SHIFT);
        // Every `Token` retains nonzero target bits independently of
        // its kind, so replacing the kind cannot produce zero.
        Self::from_proven_target(raw)
    }

    const fn pack(route: u8, slot: SlotIndex, epoch: u32) -> u64 {
        ((route as u64) << ROUTE_SHIFT) | ((epoch as u64) << EPOCH_SHIFT) | slot.0 as u64
    }

    const fn from_proven_target(raw: u64) -> Self {
        Self(
            // SAFETY: this private wrapper's callers prove that the
            // kind-independent target is nonzero.
            unsafe { NonZeroU64::new_unchecked(raw) },
            PhantomData,
        )
    }

    pub const fn try_from_raw(raw: u64) -> Option<Self> {
        if raw & TARGET_MASK == 0 {
            return None;
        }
        Some(Self::from_proven_target(raw))
    }

    pub const fn raw(self) -> u64 {
        self.0.get()
    }

    pub const fn route(self) -> u8 {
        (self.0.get() >> ROUTE_SHIFT) as u8
    }

    pub const fn kind(self) -> u8 {
        ((self.0.get() & KIND_MASK) >> KIND_SHIFT) as u8
    }

    pub const fn slot(self) -> SlotIndex {
        SlotIndex::from_bounded((self.0.get() & SLOT_MASK) as u32)
    }

    pub const fn epoch(self) -> Option<Epoch> {
        Epoch::new(self.epoch_raw())
    }

    pub const fn epoch_raw(self) -> u32 {
        ((self.0.get() >> EPOCH_SHIFT) & EPOCH_MASK) as u32
    }

    pub const fn same_target(self, other: Self) -> bool {
        self.0.get() & !KIND_MASK == other.0.get() & !KIND_MASK
    }

    #[allow(private_bounds)]
    pub const fn parts<Tag: TokenTag>(self) -> Option<KeyParts<Tag>> {
        if self.raw() & Tag::MASK != Tag::VALUE {
            return None;
        }
        KeyParts::new(self.slot().raw(), self.epoch_raw())
    }

    #[allow(private_bounds)]
    pub const fn from_key<Tag: TokenTag>(key: Key<Tag>) -> Self {
        let raw = Self::pack(Tag::ROUTE, key.slot(), key.generation().get())
            | ((Tag::KIND as u64) << KIND_SHIFT);
        // `SlabGeneration` proves that the target is nonzero.
        Self::from_proven_target(raw)
    }

    #[doc(hidden)]
    #[allow(private_bounds)]
    pub const fn from_parts<Tag: TokenTag>(parts: KeyParts<Tag>) -> Self {
        let raw = Self::pack(Tag::ROUTE, parts.slot(), parts.generation().get())
            | ((Tag::KIND as u64) << KIND_SHIFT);
        // `KeyParts` proves that the generation, and therefore the
        // kind-independent target, is nonzero.
        Self::from_proven_target(raw)
    }
}

pub const SHUTDOWN: Token = Token::framework(SlotIndex::ZERO).with_kind(kind::SHUTDOWN);
