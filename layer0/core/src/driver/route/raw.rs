use core::{convert, mem, num};

use o3::{
    base::marker::ThreadBound,
    depot,
    depot::{
        arena::slab::{
            self, entries, external,
            external::{errors, shared},
            lookup,
        },
        capacity,
        index::Initialized,
    },
};

use crate::driver::{
    route,
    route::{indices, table},
};

/// Niche-backed target identity for unchecked keyed storage.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Identity<'d, Tag: route::Tag>(Option<route::Target<'d, Tag>>);

// SAFETY: every raw value through SLOT_MASK is one complete route slot;
// scalar conversions are deterministic and cannot panic or reenter storage.
unsafe impl depot::raw::Compact for indices::SlotIndex {
    const MAX: u32 = route::SLOT_MASK as u32;

    fn into_raw(self) -> u32 {
        self.raw()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        indices::SlotIndex::from_field(u64::from(raw))
    }
}

// SAFETY: every position admitted by Capacity fits the route slot encoding.
unsafe impl depot::raw::Count<indices::SlotIndex> for table::Capacity {
    fn count(self) -> capacity::Count {
        self.count()
    }
}

/// Raw consuming access to one shared route table.
#[repr(transparent)]
pub struct Cell<'a, T, Tag: route::Tag> {
    inner: &'a shared::Storage<T, route::Epoch, Tag>,
}

impl<'a, T, Tag: route::Tag> Cell<'a, T, Tag> {
    pub(in crate::driver::route) const fn new(
        inner: &'a shared::Storage<T, route::Epoch, Tag>,
    ) -> Self {
        Self { inner }
    }
}

type Allocation<'a, T, Tag> = depot::raw::Proof<
    (&'a shared::Storage<T, route::Epoch, Tag>, u32),
    Initialized<entries::Allocated>,
>;

type Vacancy<'a, T, Tag> = external::Vacant<
    entries::Pending<
        'a,
        external::Stamped<T, route::Epoch>,
        { u32::MAX },
        entries::Interior<true>,
        1,
    >,
    Tag,
    true,
    route::Epoch,
>;

/// A position admitted in the retained route storage allocation.
pub struct Slot<'a, T, Tag: route::Tag>(Allocation<'a, T, Tag>);

impl<'a, T, Tag: route::Tag> Slot<'a, T, Tag> {
    pub fn vacancy(self) -> Result<Vacancy<'a, T, Tag>, slab::Vacancy> {
        self.0.vacancy()
    }

    pub fn try_entry(
        self,
    ) -> Result<table::Current<'a, T, Tag>, lookup::Error<(), convert::Infallible>> {
        Ok(table::Current::new(self.0.try_entry()?))
    }

    pub fn try_key(
        self,
        epoch: route::Epoch,
    ) -> Result<table::Current<'a, T, Tag>, errors::Lookup<route::Epoch, Tag, ()>> {
        Ok(table::Current::new(self.0.try_key(epoch)?))
    }
}

// SAFETY: this projects the exact retained route table's allocated position.
unsafe impl<'a, T, Tag: route::Tag> depot::raw::Project<indices::SlotIndex, entries::Allocated>
    for Cell<'a, T, Tag>
{
    type Output = Slot<'a, T, Tag>;

    unsafe fn project(self, index: indices::SlotIndex) -> Self::Output {
        // SAFETY: the consumed proof retains this identical backing allocation.
        let live = unsafe { depot::raw::Proof::new((self.inner, index.raw())) };
        Slot(live.into())
    }
}

impl<'d, Tag: route::Tag> Identity<'d, Tag> {
    pub const VACANT: Self = Self(None);

    pub const fn from_target(target: route::Target<'d, Tag>) -> Self {
        Self(Some(target))
    }

    pub fn into_target(self) -> Option<route::Target<'d, Tag>> {
        self.0
    }

    /// Reconstructs the exact target retained by a live identity.
    /// # Safety
    /// This identity must have been produced by [`Self::from_target`].
    pub unsafe fn target(self) -> route::Target<'d, Tag> {
        // SAFETY: the caller proves the niche is occupied by an exact Target.
        unsafe { mem::transmute(self.0) }
    }
}

impl<Tag: route::Tag> PartialEq for Identity<'_, Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<Tag: route::Tag> Eq for Identity<'_, Tag> {}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Auxiliary(u32);

impl Auxiliary {
    pub(crate) const fn from_meta(meta: u64) -> Self {
        Self(((meta & route::AUX_MASK) >> route::AUX_SHIFT) as u32)
    }

    pub(crate) const fn with_bits(self, mask: u8, value: u8) -> Self {
        let mask = mask as u32;
        Self((self.0 & !mask) | (value as u32 & mask))
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0
    }
}

impl From<u8> for Auxiliary {
    fn from(raw: u8) -> Self {
        Self(u32::from(raw))
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct Packed {
    identity: num::NonZero<u64>,
    thread: ThreadBound,
    meta: u64,
}

impl Packed {
    pub(crate) const fn new(token: route::Token, aux: Auxiliary) -> Self {
        let route::Token {
            epoch,
            thread,
            meta,
        } = token;
        Self {
            identity: epoch,
            thread,
            meta: (meta & !route::AUX_MASK) | ((aux.raw() as u64) << route::AUX_SHIFT),
        }
    }

    pub(crate) const fn words(self) -> [u64; 2] {
        [self.identity.get(), self.meta]
    }

    pub(crate) const fn from_words(
        identity: num::NonZero<u64>,
        meta: u64,
        thread: ThreadBound,
    ) -> Self {
        Self {
            identity,
            thread,
            meta,
        }
    }

    /// # Safety
    /// `words` must have been written by [`Self::words`] for a dispatch payload.
    pub(crate) const unsafe fn restore(words: [u64; 2], thread: ThreadBound) -> Self {
        Self::from_words(
            unsafe { num::NonZero::<u64>::new_unchecked(words[0]) },
            words[1],
            thread,
        )
    }

    pub(crate) const fn target(self) -> route::Token {
        route::Token {
            epoch: self.identity,
            thread: self.thread,
            meta: self.meta & !route::AUX_MASK,
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Identity<'static, route::KeyTag<1>>>()
            == mem::size_of::<route::Target<'static, route::KeyTag<1>>>()
    );
    assert!(
        mem::align_of::<Identity<'static, route::KeyTag<1>>>()
            == mem::align_of::<route::Target<'static, route::KeyTag<1>>>()
    );
    assert!(
        mem::size_of::<Identity<'static, route::KeyTag<1>>>() == mem::size_of::<route::Token>()
    );
    assert!(
        mem::align_of::<Identity<'static, route::KeyTag<1>>>() == mem::align_of::<route::Token>()
    );
    assert!(mem::size_of::<Auxiliary>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Packed>() == 2 * mem::size_of::<u64>());
    assert!(mem::align_of::<Packed>() == mem::align_of::<u64>());
};
