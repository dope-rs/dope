use crate::errors::detail;
use std::num;

use crate::conn::id;
use crate::conn::stream::resident;
use crate::conn::stream::state::receive::recv;
use crate::conn::wire::ingress::frames;
use crate::wire::stream::Direction;
use crate::wire::varint;
use recv::map;

const FLAG_BITS: u32 = u8::BITS;
const MAX_ORIGIN: u64 = u64::MAX >> FLAG_BITS;
const MAX_DATA_DIRTY: u8 = 1 << 0;
const MAX_STREAMS_BIDI_DIRTY: u8 = 1 << 1;
const MAX_STREAMS_UNI_DIRTY: u8 = 1 << 2;
const CONTROL_DIRTY: u8 = MAX_DATA_DIRTY | MAX_STREAMS_BIDI_DIRTY | MAX_STREAMS_UNI_DIRTY;

/// Receive-wide identity and flags packed into the word that previously held
/// one byte of flags and alignment padding.
#[repr(transparent)]
pub(in crate::conn) struct Meta(id::Local<num::NonZero<u64>>);

impl Meta {
    pub(in crate::conn) fn try_new() -> Result<Self, resident::Exhausted> {
        let origin = resident::Origin::try_new(MAX_ORIGIN)?;
        let packed = (origin.raw() - 1) << FLAG_BITS;
        Ok(Self(id::Local::new(
            num::NonZero::<u64>::MIN.saturating_add(packed),
        )))
    }

    pub(in crate::conn) fn origin(&self) -> resident::Origin {
        resident::Origin::from_index((self.0.value().get() - 1) >> FLAG_BITS)
    }

    pub(in crate::conn) const fn contains(&self, flag: u8) -> bool {
        (self.0.value().get() - 1) & flag as u64 != 0
    }

    pub(in crate::conn) fn mark(&mut self, flag: u8) {
        let packed = (self.0.value().get() - 1) | u64::from(flag);
        *self.0.value_mut() = num::NonZero::<u64>::MIN.saturating_add(packed);
    }

    pub(in crate::conn) fn clear(&mut self, flag: u8) {
        let packed = (self.0.value().get() - 1) & !u64::from(flag);
        *self.0.value_mut() = num::NonZero::<u64>::MIN.saturating_add(packed);
    }

    pub(super) const fn has_ctrl(&self) -> bool {
        self.contains(CONTROL_DIRTY)
    }

    pub(super) const fn has_data(&self) -> bool {
        self.contains(MAX_DATA_DIRTY)
    }

    pub(super) fn mark_data(&mut self) {
        self.mark(MAX_DATA_DIRTY);
    }

    pub(super) fn clear_data(&mut self) {
        self.clear(MAX_DATA_DIRTY);
    }

    pub(super) const fn has_stream(&self, dir: Direction) -> bool {
        self.contains(max_streams_dirty(dir))
    }

    pub(super) fn mark_streams(&mut self, dir: Direction) {
        self.mark(max_streams_dirty(dir));
    }

    pub(super) fn clear_stream(&mut self, dir: Direction) {
        self.clear(max_streams_dirty(dir));
    }
}

const fn max_streams_dirty(dir: Direction) -> u8 {
    *dir.bind(&[MAX_STREAMS_BIDI_DIRTY, MAX_STREAMS_UNI_DIRTY])
}

#[repr(C)]
pub(in crate::conn) struct Flow {
    desired: varint::VarInt,
    published: varint::VarInt,
    total: u64,
}

#[repr(transparent)]
pub(in crate::conn) struct Recorded(u64);

impl Flow {
    pub(in crate::conn) fn new(limit: varint::VarInt) -> Self {
        Self {
            desired: limit,
            published: limit,
            total: 0,
        }
    }

    pub(in crate::conn) const fn desired(&self) -> varint::VarInt {
        self.desired
    }

    pub(in crate::conn) fn publish(&mut self, limit: varint::VarInt) {
        self.published = limit;
    }

    pub(in crate::conn) fn try_record(&self, count: u64) -> Result<Recorded, detail::Flow> {
        let error = || detail::Flow {
            current: self.total,
            count,
            limit: self.published,
        };
        let total = self.total.checked_add(count).ok_or_else(error)?;
        if total > self.published.get() {
            return Err(error());
        }
        Ok(Recorded(total))
    }

    pub(in crate::conn) fn record(
        &mut self,
        frames::CommitPermit { .. }: frames::CommitPermit<'_>,
        recorded: Recorded,
    ) {
        self.total = recorded.0;
    }

    pub(in crate::conn) fn release(&mut self, count: u64) -> Option<varint::VarInt> {
        let next = varint::VarInt::saturate(u128::from(self.desired.get()) + u128::from(count));
        if next == self.desired {
            return None;
        }
        self.desired = next;
        Some(next)
    }
}

#[repr(C)]
pub(in crate::conn) struct Window {
    released: varint::VarInt,
    published: Option<varint::Stored>,
}

impl Window {
    pub(in crate::conn) fn new() -> Self {
        Self {
            released: varint::VarInt::ZERO,
            published: None,
        }
    }

    pub(in crate::conn) fn reuse(&mut self) {
        *self = Self::new();
    }

    pub(in crate::conn) fn limit(&self, base: varint::VarInt) -> varint::VarInt {
        match self.published {
            Some(limit) => limit.get(),
            None => base,
        }
    }

    pub(in crate::conn) fn desired(&self, base: varint::VarInt) -> varint::VarInt {
        varint::VarInt::saturate(u128::from(base.get()) + u128::from(self.released.get()))
    }

    pub(in crate::conn) fn publish(&mut self, limit: varint::VarInt) {
        self.published = Some(limit.into());
    }

    pub(in crate::conn) fn release(
        &mut self,
        base: varint::VarInt,
        count: u64,
    ) -> Option<varint::VarInt> {
        let prev = self.desired(base);
        self.released =
            varint::VarInt::saturate(u128::from(self.released.get()) + u128::from(count));
        let next = self.desired(base);
        (next != prev).then_some(next)
    }

    pub(in crate::conn) fn has_held(&self, seen: u64) -> bool {
        seen != self.released.get()
    }
}

#[derive(Debug)]
pub(in crate::conn) struct Receipt<T = map::Credit> {
    origin: resident::Origin,
    value: T,
}

impl<T> Receipt<T> {
    pub(in crate::conn) const fn new(origin: resident::Origin, value: T) -> Self {
        Self { origin, value }
    }

    pub(in crate::conn) fn belongs_to(&self, meta: &Meta) -> bool {
        self.origin == meta.origin()
    }

    pub(in crate::conn) const fn origin(&self) -> resident::Origin {
        self.origin
    }

    pub(in crate::conn) const fn value(&self) -> &T {
        &self.value
    }

    pub(in crate::conn) fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub(in crate::conn) fn into_parts(self) -> (resident::Origin, T) {
        (self.origin, self.value)
    }
}

const _: () = assert!(std::mem::size_of::<Recorded>() == std::mem::size_of::<u64>());
const _: () = assert!(std::mem::size_of::<Meta>() == std::mem::size_of::<u64>());
const _: () = assert!(std::mem::size_of::<Flow>() == 3 * std::mem::size_of::<u64>());
const _: () = assert!(std::mem::size_of::<Window>() == 2 * std::mem::size_of::<u64>());
const _: () = assert!(std::mem::size_of::<Receipt>() == 3 * std::mem::size_of::<u64>());
