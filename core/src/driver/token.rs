use core::marker::PhantomData;
use core::num::NonZeroU64;

use o3::collections::{CellSlab, Slab, SlabKey, SlabKeyParts};

pub const ROUTE_FRAMEWORK: u8 = 255;

pub const ROUTE_SHIFT: u32 = 56;
pub const KIND_SHIFT: u32 = 48;
const EPOCH_SHIFT: u32 = 24;
const ROUTE_MASK: u64 = 0xFF << ROUTE_SHIFT;
const KIND_MASK: u64 = 0xFF << KIND_SHIFT;
pub const SLOT_BITS: u32 = 24;
const EPOCH_BITS: u32 = 24;
pub const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
pub const EPOCH_MASK: u64 = (1 << EPOCH_BITS) - 1;

pub type Key<Tag> = SlabKey<Tag, { EPOCH_MASK as u32 }>;
pub type TokenCellSlab<T, Tag> = CellSlab<T, Tag, { EPOCH_MASK as u32 }>;
pub type TokenSlab<T, Tag> = Slab<T, Tag, { EPOCH_MASK as u32 }>;

#[repr(transparent)]
pub struct KeyParts<Tag> {
    parts: SlabKeyParts<{ EPOCH_MASK as u32 }>,
    tag: PhantomData<*mut Tag>,
}

impl<Tag> Clone for KeyParts<Tag> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag> Copy for KeyParts<Tag> {}

impl<Tag> KeyParts<Tag> {
    pub const fn index(self) -> u32 {
        self.parts.index()
    }

    pub const fn slab(self) -> SlabKeyParts<{ EPOCH_MASK as u32 }> {
        self.parts
    }
}

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
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Default)]
#[repr(transparent)]
pub struct SlotIndex(u32);

impl SlotIndex {
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Default)]
#[repr(transparent)]
pub struct Epoch(u32);

impl Epoch {
    pub const INITIAL: Self = Self(1);
    pub const MAX: Self = Self(EPOCH_MASK as u32);
    pub const ZERO: Self = Self(0);

    pub const fn new(raw: u32) -> Option<Self> {
        if raw <= EPOCH_MASK as u32 {
            Some(Self(raw))
        } else {
            None
        }
    }

    pub const fn raw(self) -> u32 {
        self.0
    }

    pub const fn next(self) -> Option<Self> {
        if self.0 < EPOCH_MASK as u32 {
            Some(Self(self.0 + 1))
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
        assert!((slot.0 as u64) <= SLOT_MASK, "dope: token slot overflow");
        assert!((epoch.0 as u64) <= EPOCH_MASK, "dope: token epoch overflow");
        let raw = ((route as u64) << ROUTE_SHIFT)
            | ((epoch.0 as u64) << EPOCH_SHIFT)
            | (slot.0 as u64 & SLOT_MASK);
        Self::from_nonzero(raw)
    }

    pub const fn with_kind(self, kind: u8) -> Self {
        let cleared = self.0.get() & !((0xFFu64) << KIND_SHIFT);
        Self::from_nonzero(cleared | ((kind as u64) << KIND_SHIFT))
    }

    const fn from_nonzero(raw: u64) -> Self {
        assert!(
            raw != 0,
            "dope: token requires a nonzero route, slot, or epoch"
        );
        Self(unsafe { NonZeroU64::new_unchecked(raw) }, PhantomData)
    }

    pub const fn try_from_raw(raw: u64) -> Option<Self> {
        match NonZeroU64::new(raw) {
            Some(v) => Some(Self(v, PhantomData)),
            None => None,
        }
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
        SlotIndex((self.0.get() & SLOT_MASK) as u32)
    }

    pub const fn epoch(self) -> Epoch {
        Epoch(((self.0.get() >> EPOCH_SHIFT) & EPOCH_MASK) as u32)
    }

    pub const fn same_target(self, other: Self) -> bool {
        self.0.get() & !KIND_MASK == other.0.get() & !KIND_MASK
    }

    #[allow(private_bounds)]
    pub const fn parts<Tag: TokenTag>(self) -> Option<KeyParts<Tag>> {
        if self.raw() & Tag::MASK != Tag::VALUE {
            return None;
        }
        match SlabKeyParts::new(self.slot().raw(), self.epoch().raw()) {
            Some(parts) => Some(KeyParts {
                parts,
                tag: PhantomData,
            }),
            None => None,
        }
    }

    #[allow(private_bounds)]
    pub const fn from_key<Tag: TokenTag>(key: Key<Tag>) -> Self {
        Self::new(
            Tag::ROUTE,
            SlotIndex::new(key.index()),
            Epoch(key.generation().get()),
        )
        .with_kind(Tag::KIND)
    }
}

pub const SHUTDOWN: Token =
    Token::new(ROUTE_FRAMEWORK, SlotIndex::new(0), Epoch::ZERO).with_kind(kind::SHUTDOWN);
