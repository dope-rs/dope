use std::mem;

use o3::{
    base::num::bounded,
    depot,
    depot::{arena::slab::key, queue::links},
};

use crate::reply::layout;

pub(in crate::reply::arena::state) enum EntryTag {}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(in crate::reply) struct EntryKey(key::Handle<EntryTag>);

impl EntryKey {
    /// # Safety
    /// The handle must come from this reply layout's bounded entry store.
    pub(super) const unsafe fn from_raw(raw: key::Handle<EntryTag>) -> Self {
        Self(raw)
    }

    pub(super) const fn raw(self) -> key::Handle<EntryTag> {
        self.0
    }

    pub(in crate::reply) fn index(self) -> Index {
        // SAFETY: EntryKey construction retains this bounded entry-store index.
        unsafe { depot::raw::Compact::from_raw(self.0.index()) }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(in crate::reply) struct Index(bounded::U32<0, { layout::MAXIMUM_ENTRIES.get() as u32 - 1 }>);

impl Index {
    pub(in crate::reply) const fn get(self) -> usize {
        self.0.into_usize()
    }
}

impl From<Index> for links::Index {
    fn from(index: Index) -> Self {
        Self::from(index.0)
    }
}

// SAFETY: both conversions preserve the same bounded entry value unchanged.
unsafe impl depot::raw::Compact for Index {
    const MAX: u32 = layout::MAXIMUM_ENTRIES.get() as u32 - 1;

    fn into_raw(self) -> u32 {
        self.0.get()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        // SAFETY: Compact requires raw within this exact inclusive bound.
        Self(unsafe { bounded::raw::u32(raw) })
    }
}

const _: () = {
    assert!(mem::size_of::<EntryKey>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Index>() == mem::size_of::<u32>());
};
