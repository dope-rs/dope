use std::{cell, io, mem, pin};

use o3::collections;
use ready::task;

mod pool;
pub(in crate::driver::schedule::ready) use pool::{Pool, Reservation};

use crate::driver::{route, schedule::ready, settings};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::driver::schedule::ready) enum Kind {
    Free,
    Reserved,
    Dispatch,
    Task,
    Retired,
}

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Layout {
    fixed: usize,
    dynamic: usize,
    capacity: usize,
}

impl Layout {
    pub(in crate::driver::schedule::ready) fn new(
        fixed: usize,
        dynamic: settings::ScheduleCapacity,
    ) -> io::Result<Self> {
        let dynamic = dynamic.get();
        let capacity = fixed.checked_add(dynamic).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "dope: ready capacity overflow")
        })?;
        if capacity > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: ready capacity exceeds u32",
            ));
        }
        Ok(Self {
            fixed,
            dynamic,
            capacity,
        })
    }

    pub(in crate::driver::schedule::ready) fn capacity(self) -> usize {
        self.capacity
    }

    fn first_free(self) -> FreeLink {
        if self.dynamic == 0 {
            FreeLink::EMPTY
        } else {
            FreeLink::from_index(self.free_index(self.fixed))
        }
    }

    fn next_free(self, index: usize) -> FreeLink {
        debug_assert!(index >= self.fixed);
        debug_assert!(index < self.capacity);
        let next = index + 1;
        if next == self.capacity {
            FreeLink::EMPTY
        } else {
            FreeLink::from_index(self.free_index(next))
        }
    }

    fn free_index(self, index: usize) -> FreeIndex {
        debug_assert!(index >= self.fixed);
        debug_assert!(index < self.capacity);
        FreeIndex(index as u32)
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct FreeIndex(u32);

impl FreeIndex {
    pub(in crate::driver::schedule::ready) fn get(self) -> usize {
        self.0 as usize
    }

    pub(in crate::driver::schedule::ready) fn raw(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct FreeLink(u32);

impl FreeLink {
    const EMPTY: Self = Self(u32::MAX);

    pub(super) fn is_empty(self) -> bool {
        self.0 == Self::EMPTY.0
    }

    pub(super) fn from_index(index: FreeIndex) -> Self {
        Self(index.0)
    }

    pub(super) fn index(self) -> FreeIndex {
        debug_assert!(self.0 != Self::EMPTY.0);
        FreeIndex(self.0)
    }
}

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Resolved<'a> {
    index: usize,
    payload: &'a cell::Cell<ready::raw::Payload>,
    kind: &'a cell::Cell<Kind>,
}

impl<'a> Resolved<'a> {
    pub(in crate::driver::schedule::ready) fn index(self) -> usize {
        self.index
    }

    pub(in crate::driver::schedule::ready) fn dispatch(self) -> Option<Dispatch<'a>> {
        (self.kind.get() == Kind::Dispatch).then_some(Dispatch(self.payload))
    }

    pub(in crate::driver::schedule::ready) fn entry<'d>(
        self,
        access: ready::Access<'a, 'd>,
    ) -> Entry<'a, 'd>
    where
        'd: 'a,
    {
        entry(self.payload, self.kind.get(), access)
    }
}

pub(in crate::driver::schedule::ready) enum Entry<'a, 'd>
where
    'd: 'a,
{
    Vacant,
    Dispatch(route::Token),
    Task(pin::Pin<&'a task::Node<'d>>),
}

pub(in crate::driver::schedule::ready) struct Dispatch<'a>(&'a cell::Cell<ready::raw::Payload>);

impl Dispatch<'_> {
    pub(in crate::driver::schedule::ready) fn get(&self) -> route::Token {
        unsafe { self.0.get().into_dispatch() }
    }

    pub(in crate::driver::schedule::ready) fn set(&self, target: route::Token) {
        self.0.set(ready::raw::Payload::dispatch(target));
    }
}

pub(in crate::driver::schedule::ready) struct Dynamic<'a> {
    pub(super) index: FreeIndex,
    pub(super) payload: &'a cell::Cell<ready::raw::Payload>,
    pub(super) kind: &'a cell::Cell<Kind>,
    pub(super) epoch: &'a cell::Cell<u32>,
}

pub(in crate::driver::schedule::ready) struct Slots {
    fixed: usize,
    payloads: Box<[cell::Cell<ready::raw::Payload>]>,
    kinds: Box<[cell::Cell<Kind>]>,
    epochs: Box<[cell::Cell<u32>]>,
}

pub(in crate::driver::schedule::ready) struct Table {
    pub(in crate::driver::schedule::ready) slots: Slots,
    pub(in crate::driver::schedule::ready) pool: Pool,
}

impl Table {
    pub(in crate::driver::schedule::ready) fn try_new(
        layout: Layout,
        dummy: route::Token,
    ) -> Result<Self, collections::AllocationError> {
        let slots = Slots {
            fixed: layout.fixed,
            payloads: collections::BoxSliceExt::try_box_with(layout.capacity, |index| {
                let payload = if index < layout.fixed {
                    ready::raw::Payload::dispatch(dummy)
                } else {
                    ready::raw::Payload::free(layout.next_free(index))
                };
                cell::Cell::new(payload)
            })?,
            kinds: collections::BoxSliceExt::try_box_with(layout.capacity, |index| {
                cell::Cell::new(if index < layout.fixed {
                    Kind::Reserved
                } else {
                    Kind::Free
                })
            })?,
            epochs: collections::BoxSliceExt::try_box_with(layout.capacity, |_| {
                cell::Cell::new(0)
            })?,
        };
        let pool = Pool::new(layout.first_free(), layout.dynamic);
        Ok(Self { slots, pool })
    }
}

impl Slots {
    pub(in crate::driver::schedule::ready) fn is_fixed(&self, index: usize) -> bool {
        index < self.fixed
    }

    pub(in crate::driver::schedule::ready) fn resolve(
        &self,
        key: ready::Key<'_>,
    ) -> Option<Resolved<'_>> {
        let index = key.index as usize;
        let payload = self.payloads.get(index)?;
        let kind = unsafe { self.kinds.get_unchecked(index) };
        if self.epochs.get(index)?.get() != key.epoch {
            return None;
        }
        Some(Resolved {
            index,
            payload,
            kind,
        })
    }

    pub(in crate::driver::schedule::ready) fn claim_fixed(
        &self,
        index: usize,
        target: route::Token,
    ) -> Option<u32> {
        if index >= self.fixed {
            return None;
        }
        let kind = &self.kinds[index];
        if kind.get() != Kind::Reserved {
            return None;
        }
        self.payloads[index].set(ready::raw::Payload::dispatch(target));
        kind.set(Kind::Dispatch);
        Some(self.epochs[index].get())
    }

    pub(in crate::driver::schedule::ready) fn release_fixed(
        &self,
        key: ready::FixedKey<'_>,
        remove_ready: impl FnOnce(usize) -> bool,
    ) -> bool {
        let index = key.index() as usize;
        if index >= self.fixed
            || self.epochs[index].get() != key.key().epoch
            || self.kinds[index].get() != Kind::Dispatch
        {
            return false;
        }
        self.kinds[index].set(Kind::Reserved);
        remove_ready(index);
        let Some(epoch) = self.epochs[index].get().checked_add(1) else {
            self.kinds[index].set(Kind::Retired);
            return true;
        };
        self.epochs[index].set(epoch);
        true
    }

    pub(in crate::driver::schedule::ready) fn entry<'a, 'd>(
        &'a self,
        index: usize,
        access: ready::Access<'a, 'd>,
    ) -> Entry<'a, 'd>
    where
        'd: 'a,
    {
        entry(&self.payloads[index], self.kinds[index].get(), access)
    }

    pub(super) fn dynamic(&self, index: FreeIndex) -> Dynamic<'_> {
        let absolute = index.get();
        debug_assert!(absolute >= self.fixed);
        debug_assert!(absolute < self.payloads.len());
        unsafe {
            Dynamic {
                index,
                payload: self.payloads.get_unchecked(absolute),
                kind: self.kinds.get_unchecked(absolute),
                epoch: self.epochs.get_unchecked(absolute),
            }
        }
    }
}

fn entry<'a, 'd>(
    payload: &'a cell::Cell<ready::raw::Payload>,
    kind: Kind,
    access: ready::Access<'a, 'd>,
) -> Entry<'a, 'd>
where
    'd: 'a,
{
    match kind {
        Kind::Free | Kind::Reserved | Kind::Retired => Entry::Vacant,
        Kind::Dispatch => Entry::Dispatch(unsafe { payload.get().into_dispatch() }),
        Kind::Task => Entry::Task(unsafe { payload.get().into_task(access) }),
    }
}

const _: () = {
    assert!(mem::size_of::<FreeIndex>() == mem::size_of::<u32>());
    assert!(mem::size_of::<FreeLink>() == mem::size_of::<u32>());
    assert!(
        mem::size_of::<cell::Cell<ready::raw::Payload>>() == 2 * mem::size_of::<cell::Cell<u64>>()
    );
};
