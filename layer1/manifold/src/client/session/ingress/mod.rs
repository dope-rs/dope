use std::{mem::size_of, num};

use dope_core::driver::{self, storage, storage::Exhausted};
use hold::limits;
use o3::{
    buffer::{hold, resident},
    depot::capacity,
};

/// Sealed ownership policies for ingress retention sources.
pub mod origin;

/// Exact external resident authority used by ingress retention.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Admit<'d>(resident::Budget<'d>);

/// No shared ingress owner.
#[derive(Clone, Copy)]
pub struct None;

/// In-process shared ingress owners which acquire their own resident authority
/// when output escapes incremental staging.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Local(capacity::Count);

/// Externally budgeted shared ingress owners.
#[derive(Clone, Copy)]
pub struct In<'d> {
    admit: Admit<'d>,
    owners: capacity::Count,
}

/// Byte capacity retained for session ingress output.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Bytes(usize);

/// Shared warm-buffer count bounded by the configured owner count.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Buffers(capacity::Count);

/// A statically proven ingress retention source.
pub trait Source<'d>: Copy + origin::Seal {
    /// Resident authority policy carried by this source.
    type Authority: origin::Policy;

    /// Computes shared retained capacity for the current warm allocation.
    fn shared(self, retained: Bytes, warm: num::NonZero<u32>) -> Buffers;
    /// Reserves source-specific retained capacity.
    fn reserve(self, retained: Bytes) -> Bytes;
    /// Returns whether retained bytes are charged to external authority.
    fn is_accounted(self) -> bool;
    /// Acquires exact resident authority for one retained capacity.
    fn acquire(
        self,
        capacity: Bytes,
        driver: &driver::Context<'_, 'd>,
    ) -> Result<resident::Budget<'d>, Exhausted>;
}

/// Retained-output bytes coupled to their statically proven source.
#[derive(Clone, Copy)]
pub struct Plan<S> {
    retained: Bytes,
    source: S,
}

impl<'d> Admit<'d> {
    /// Wraps externally supplied resident authority.
    pub const fn new(budget: resident::Budget<'d>) -> Self {
        Self(budget)
    }

    pub fn capacity(self) -> Bytes {
        Bytes::new(self.0.capacity())
    }
}

impl<'d> Source<'d> for None {
    type Authority = origin::Local;

    fn shared(self, _: Bytes, _: num::NonZero<u32>) -> Buffers {
        Buffers::EMPTY
    }

    fn reserve(self, retained: Bytes) -> Bytes {
        retained
    }

    fn is_accounted(self) -> bool {
        false
    }

    fn acquire(
        self,
        capacity: Bytes,
        driver: &driver::Context<'_, 'd>,
    ) -> Result<resident::Budget<'d>, Exhausted> {
        driver.storage.budget(storage::Resident::bytes(capacity.0))
    }
}

impl origin::Seal for None {}

impl Local {
    pub const fn new(owners: capacity::Count) -> Self {
        Self(owners)
    }
}

impl<'d> Source<'d> for Local {
    type Authority = origin::Local;

    fn shared(self, retained: Bytes, warm: num::NonZero<u32>) -> Buffers {
        let count = retained.0 / warm.get() as usize;
        if count >= self.0.get() {
            return Buffers(self.0);
        }
        Buffers(capacity::Count::new(count as u32))
    }

    fn reserve(self, retained: Bytes) -> Bytes {
        retained
    }

    fn is_accounted(self) -> bool {
        true
    }

    fn acquire(
        self,
        capacity: Bytes,
        driver: &driver::Context<'_, 'd>,
    ) -> Result<resident::Budget<'d>, Exhausted> {
        driver.storage.budget(storage::Resident::bytes(capacity.0))
    }
}

impl origin::Seal for Local {}

impl<'d> In<'d> {
    pub const fn new(admit: Admit<'d>, owners: capacity::Count) -> Self {
        Self { admit, owners }
    }
}

impl<'d> Source<'d> for In<'d> {
    type Authority = origin::External;

    fn shared(self, _: Bytes, warm: num::NonZero<u32>) -> Buffers {
        Local::new(self.owners).shared(self.admit.capacity(), warm)
    }

    fn reserve(self, _: Bytes) -> Bytes {
        self.admit.capacity()
    }

    fn is_accounted(self) -> bool {
        true
    }

    fn acquire(
        self,
        _: Bytes,
        _: &driver::Context<'_, 'd>,
    ) -> Result<resident::Budget<'d>, Exhausted> {
        Ok(self.admit.0)
    }
}

impl origin::Seal for In<'_> {}

impl Plan<None> {
    pub const fn none() -> Self {
        Self {
            retained: Bytes::EMPTY,
            source: None,
        }
    }
}

impl Plan<Local> {
    pub const fn local(retained: Bytes, owners: capacity::Count) -> Self {
        Self {
            retained,
            source: Local::new(owners),
        }
    }
}

impl<'d> Plan<In<'d>> {
    pub const fn external(admit: Admit<'d>, owners: capacity::Count) -> Self {
        Self {
            retained: Bytes::EMPTY,
            source: In::new(admit, owners),
        }
    }
}

impl<S> Plan<S> {
    pub(super) fn shared<'d>(self, warm: num::NonZero<u32>) -> Buffers
    where
        S: Source<'d>,
    {
        self.source.shared(self.retained, warm)
    }

    pub(super) fn reserve<'d>(self) -> Bytes
    where
        S: Source<'d>,
    {
        self.source.reserve(self.retained)
    }

    pub(super) fn is_accounted<'d>(self) -> bool
    where
        S: Source<'d>,
    {
        self.source.is_accounted()
    }

    pub(super) fn acquire<'d>(
        self,
        capacity: Bytes,
        driver: &driver::Context<'_, 'd>,
    ) -> Result<resident::Budget<'d>, Exhausted>
    where
        S: Source<'d>,
    {
        self.source.acquire(capacity, driver)
    }
}

impl Bytes {
    pub const EMPTY: Self = Self(0);

    pub const fn new(bytes: usize) -> Self {
        Self(bytes)
    }

    pub const fn from_limit(limit: limits::Bound) -> Self {
        Self(limit.bytes())
    }

    pub(in crate::client::session) const fn into_raw(self) -> usize {
        self.0
    }
}

impl Buffers {
    pub const EMPTY: Self = Self(capacity::Count::EMPTY);

    pub(in crate::client::session) const fn into_raw(self) -> usize {
        self.0.get()
    }
}

const _: () = assert!(size_of::<Admit<'static>>() == size_of::<resident::Budget<'static>>());
const _: () = assert!(size_of::<None>() == 0);
const _: () = assert!(size_of::<Local>() == size_of::<capacity::Count>());
const _: () = assert!(size_of::<Plan<None>>() == size_of::<usize>());
