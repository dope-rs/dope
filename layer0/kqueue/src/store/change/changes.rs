use std::{cell::Cell, marker, ptr};

use dope_sys::{
    Budget,
    table::change::{self, Filter, Id as _},
};
use o3::{
    alloc,
    depot::{
        capacity,
        index::{self, bitmap, set},
    },
    mem::quota,
};

use crate::store::change::{Delete, Entry, Id, Interest, Tail, event};

pub struct Changes<S: change::raw::Slots> {
    pub(super) slots: Box<[[Cell<Option<libc::kevent>>; 2]]>,
    pub(super) pending: set::State<(), usize, 2>,
    pub(super) count: Cell<usize>,
    pub(super) staged: Box<[libc::kevent]>,
    pub(super) staged_len: usize,
    /// False proves no staged deletion; mutable slice access invalidates it.
    pub(super) staged_delete: bool,
    domain: marker::PhantomData<S::Capacity>,
}

impl<S: change::raw::Slots> change::Changes for Changes<S> {
    type Slots = S;
    type Id = Id<S::Physical>;
    type Read<'a>
        = Interest<'a, S, { libc::EVFILT_READ }>
    where
        Self: 'a;
    type Write<'a>
        = Interest<'a, S, { libc::EVFILT_WRITE }>
    where
        Self: 'a;
    type Tail<'a>
        = Tail<'a, S>
    where
        Self: 'a;
    type Deletion<'a>
        = Delete<'a, S>
    where
        Self: 'a;

    /// Allocates two event lanes per bounded slot; staging never reallocates.
    fn try_new(capacity: S::Capacity) -> Result<Self, alloc::Error> {
        const { assert!(S::MAX <= libc::c_int::MAX as usize / 2) };
        let count = S::count(capacity);
        let events = count * 2;
        let slots = alloc::BoxSliceExt::try_init(count, || [const { Cell::new(None) }; 2])?;
        Ok(Self {
            slots,
            pending: set::State::try_new(
                capacity::Count::new(events as u32).into(),
                [bitmap::Fill::Empty; 2],
            )?,
            count: Cell::new(0),
            staged: alloc::BoxSliceExt::try_init(events, || event(0, 0, 0, ptr::null_mut()))?,
            staged_len: 0,
            staged_delete: false,
            domain: marker::PhantomData,
        })
    }

    fn read(&mut self, id: Self::Id) -> Option<Self::Read<'_>> {
        self.interest(id, Filter::Read)
    }

    fn write(&mut self, id: Self::Id) -> Option<Self::Write<'_>> {
        self.interest(id, Filter::Write)
    }

    fn try_delete(
        &mut self,
        entries: impl Iterator<Item = Result<Option<(Self::Id, Filter)>, change::Index<S::Index>>>,
    ) -> Result<Self::Deletion<'_>, change::Index<S::Index>> {
        self.clear_plan();
        self.staged_len = 0;
        let plan = Delete { owner: self };
        for entry in entries {
            let Some((id, filter)) = entry? else {
                continue;
            };
            let index = id.slot().into();
            let cell = plan
                .owner
                .entry(S::offset(index), filter)
                .ok_or_else(|| change::Index::new(index, filter))?;
            let ordinal = cell.pending.index().raw();
            cell.pending.set(set::Id::at::<1>());
            // SAFETY: entry admitted this row in this owner's bitmap. Construction
            // gives that immutable domain and staging exactly two cells per slot.
            *unsafe { plan.owner.staged.get_unchecked_mut(ordinal) } = event(
                id.fd(),
                match filter {
                    Filter::Read => libc::EVFILT_READ,
                    Filter::Write => libc::EVFILT_WRITE,
                },
                libc::EV_DELETE,
                ptr::null_mut(),
            );
        }
        Ok(plan)
    }

    fn is_empty(&self) -> bool {
        self.count.get() == 0
    }

    fn len(&self) -> usize {
        self.count.get()
    }

    fn batch(&mut self, budget: &mut impl Budget) -> Self::Tail<'_> {
        self.staged_len = 0;
        self.staged_delete = false;
        let mut cursor = 0;
        for _ in 0..self.count.get() {
            let event = match budget.admit(|| {
                let (next, entry) = self.next(cursor)?;
                cursor = next;
                entry.get()
            }) {
                quota::Admission::Item(event) => event,
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            };
            self.stage(event);
        }
        Tail(self)
    }

    fn tail(&mut self, limit: usize) -> Self::Tail<'_> {
        self.staged_len = 0;
        self.staged_delete = false;
        let mut cursor = 0;
        for _ in 0..self.count.get().min(limit) {
            let Some((next, entry)) = self.next(cursor) else {
                break;
            };
            cursor = next;
            if let Some(event) = entry.get() {
                self.stage(event);
            }
        }
        Tail(self)
    }
}

impl<S: change::raw::Slots> Changes<S> {
    fn stage(&mut self, event: libc::kevent) {
        self.staged_delete |= event.flags & libc::EV_DELETE != 0;
        // SAFETY: batch and tail reset staged_len and append at most their fixed
        // entry count, which fits this owner's unchanged two-cells-per-slot storage.
        *unsafe { self.staged.get_unchecked_mut(self.staged_len) } = event;
        self.staged_len += 1;
    }

    fn interest<const FILTER: i16>(
        &mut self,
        id: Id<S::Physical>,
        filter: Filter,
    ) -> Option<Interest<'_, S, FILTER>> {
        Some(Interest {
            id,
            entry: self.entry(S::offset(id.slot().into()), filter)?,
        })
    }

    fn entry(&self, offset: usize, filter: Filter) -> Option<Entry<'_>> {
        let slot = self.slots.get(offset)?;
        let lane = usize::from(filter == Filter::Read);
        let ordinal = (self.slots.len() - 1 - offset) * 2 + lane;
        Some(Entry {
            value: &slot[lane],
            pending: self.pending.try_entry(index::Ordinal::new(ordinal)).ok()?,
            count: &self.count,
        })
    }

    pub(super) fn remove(&self, count: usize) {
        let mut cursor = 0;
        for _ in 0..self.count.get().min(count) {
            let Some((next, entry)) = self.next(cursor) else {
                break;
            };
            cursor = next;
            entry.set(None);
        }
    }

    pub(super) fn clear_plan(&self) {
        while self
            .pending
            .get::<1>()
            .pop_from(index::Ordinal::new(0))
            .is_some()
        {}
    }

    /// Finds the next active cell in the same descending slot/lane order as staging.
    pub(super) fn next(&self, start: usize) -> Option<(usize, Entry<'_>)> {
        let row = self.pending.get::<0>().find(index::Ordinal::new(start))?;
        Some((row.index().raw() + 1, self.project(row)))
    }

    /// Callers carry a row selected from this owner's immutable bitmap domain.
    pub(super) fn project<'a>(
        &'a self,
        row: set::Entry<&'a set::State<(), usize, 2>, (), usize, 2>,
    ) -> Entry<'a> {
        let ordinal = row.index().raw();
        // SAFETY: construction couples exactly two cells per slot to this bitmap.
        // Selection, deletion and admitted file-close proofs retain this exact owner.
        let slot = unsafe { self.slots.get_unchecked(self.slots.len() - 1 - ordinal / 2) };
        Entry {
            value: &slot[ordinal % 2],
            pending: row,
            count: &self.count,
        }
    }
}
