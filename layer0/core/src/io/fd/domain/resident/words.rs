use std::{cell::Cell, num::NonZero};

use o3::base::marker::ThreadBound;

use crate::{
    driver::{
        route,
        route::{indices, indices::SlotIndex},
    },
    io::fd::{
        domain,
        domain::{resident, resident::retire},
        handles,
    },
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::io::fd::domain) struct Bound(indices::SlotIndex);

#[derive(Clone, Copy)]
pub(in crate::io::fd::domain) struct Words<'a> {
    identity: &'a Cell<u64>,
    meta: &'a Cell<u64>,
    thread: ThreadBound,
}

impl Bound {
    pub(super) fn try_new(index: indices::SlotIndex, capacity: usize) -> Option<Self> {
        if index.raw() as usize >= capacity {
            return None;
        }
        Some(Self(index))
    }

    pub(super) fn mapped(index: domain::Index) -> Self {
        Self(index.0)
    }

    pub(super) fn slot(slot: handles::PhysicalSlot) -> Self {
        Self(SlotIndex::from(slot))
    }

    pub(super) fn retirement(token: &retire::Retirement) -> Self {
        Self(token.index())
    }

    pub(super) fn index(self) -> usize {
        self.0.raw() as usize
    }

    pub(super) fn cells(self, storage: &[Cell<u64>], capacity: usize) -> [&Cell<u64>; 3] {
        let index = self.index();
        let target = index * resident::TARGET_WORDS;
        let counter = capacity * resident::TARGET_WORDS + index;
        // SAFETY: Bound has no raw constructor. Its typed sources were validated
        // against the same descriptor layout that sized storage.
        unsafe {
            [
                storage.get_unchecked(target),
                storage.get_unchecked(target + 1),
                storage.get_unchecked(counter),
            ]
        }
    }
}

impl<'a> Words<'a> {
    pub(super) const fn new(
        identity: &'a Cell<u64>,
        meta: &'a Cell<u64>,
        thread: ThreadBound,
    ) -> Self {
        Self {
            identity,
            meta,
            thread,
        }
    }

    pub(super) const fn identity(self) -> &'a Cell<u64> {
        self.identity
    }

    pub(super) const fn meta(self) -> &'a Cell<u64> {
        self.meta
    }

    pub(super) fn publish(self, target: route::Token, mask: u8, dispatch: u8) {
        let aux = route::raw::Auxiliary::from_meta(self.meta.get()).with_bits(mask, dispatch);
        let packed = route::raw::Packed::new(target, aux);
        let [identity, meta] = packed.words();
        self.identity.set(identity);
        self.meta.set(meta);
    }

    pub(super) fn dispatch(self, mask: u8, dispatch: u8) -> Option<route::Token> {
        let meta = self.meta.get();
        let aux = route::raw::Auxiliary::from_meta(meta);
        if aux.raw() & u32::from(mask) != u32::from(dispatch) {
            return None;
        }
        let identity = NonZero::<u64>::new(self.identity.get())?;
        let packed = route::raw::Packed::from_words(identity, meta, self.thread);
        Some(packed.target())
    }
}

const _: () = {
    assert!(std::mem::size_of::<Bound>() == std::mem::size_of::<indices::SlotIndex>());
    assert!(std::mem::size_of::<Words<'static>>() == 2 * std::mem::size_of::<&Cell<u64>>());
};
