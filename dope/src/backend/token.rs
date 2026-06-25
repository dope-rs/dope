use std::num::NonZeroU64;

pub const ROUTE_FRAMEWORK: u8 = 255;
pub const ROUTE_TASKS: u8 = 254;

pub const ROUTE_SHIFT: u32 = 56;
pub const KIND_SHIFT: u32 = 48;
const EPOCH_SHIFT: u32 = 24;
pub const SLOT_BITS: u32 = 24;
const EPOCH_BITS: u32 = 24;
pub const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
pub const EPOCH_MASK: u64 = (1 << EPOCH_BITS) - 1;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Key {
    index: LocalIdx,
    epoch: Epoch,
}

impl Key {
    pub const fn new(index: LocalIdx, epoch: Epoch) -> Self {
        Self { index, epoch }
    }

    pub const fn index(self) -> LocalIdx {
        self.index
    }

    pub const fn epoch(self) -> Epoch {
        self.epoch
    }
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
    pub const SPLICE: u8 = 13;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Default)]
#[repr(transparent)]
pub struct LocalIdx(u32);

impl LocalIdx {
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
    pub const ZERO: Self = Self(0);

    pub const fn raw(self) -> u32 {
        self.0
    }

    pub const fn bump(self) -> Self {
        let e = self.0.wrapping_add(1) & (EPOCH_MASK as u32);
        Self(if e == 0 { 1 } else { e })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct Token(NonZeroU64);

impl Token {
    pub const fn new(route: u8, slot: LocalIdx, epoch: Epoch) -> Self {
        debug_assert!((slot.0 as u64) <= SLOT_MASK);
        let raw = ((route as u64) << ROUTE_SHIFT)
            | ((epoch.0 as u64) << EPOCH_SHIFT)
            | (slot.0 as u64 & SLOT_MASK);
        Self(NonZeroU64::new(raw).expect("Token::new: route/slot/epoch all zero"))
    }

    pub(super) const fn with_kind(self, kind: u8) -> Self {
        let cleared = self.0.get() & !((0xFFu64) << KIND_SHIFT);
        let raw = cleared | ((kind as u64) << KIND_SHIFT);
        Self(NonZeroU64::new(raw).expect("Token::with_kind: zero raw"))
    }

    pub const fn from_raw(raw: u64) -> Self {
        Self(NonZeroU64::new(raw).expect("Token::from_raw: zero raw"))
    }

    pub const fn try_from_raw(raw: u64) -> Option<Self> {
        match NonZeroU64::new(raw) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    pub const fn raw(self) -> u64 {
        self.0.get()
    }

    pub const fn route(self) -> u8 {
        (self.0.get() >> ROUTE_SHIFT) as u8
    }

    pub const fn slot(self) -> LocalIdx {
        LocalIdx((self.0.get() & SLOT_MASK) as u32)
    }

    pub const fn epoch(self) -> Epoch {
        Epoch(((self.0.get() >> EPOCH_SHIFT) & EPOCH_MASK) as u32)
    }

    pub const fn key(self) -> Key {
        Key::new(self.slot(), self.epoch())
    }

    pub const fn from_key(route: u8, key: Key) -> Self {
        Self::new(route, key.index(), key.epoch())
    }
}

pub const SHUTDOWN: Token =
    Token::new(ROUTE_FRAMEWORK, LocalIdx::new(0), Epoch::ZERO).with_kind(kind::SHUTDOWN);
