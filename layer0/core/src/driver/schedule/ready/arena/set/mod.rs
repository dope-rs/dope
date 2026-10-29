use std::{cell, mem, num};

use drain::Drain;
pub(in crate::driver::schedule::ready) use drain::Queued;
use o3::{
    alloc,
    alloc::{Error, array},
    depot::{
        self,
        index::{bitmap, bitmap::entry},
        queue::Removal,
    },
    mem::Return,
};
use state::{Phase, Side};

use crate::driver::{schedule::ready, settings};

mod drain;
mod state;

/// Ready indices isolated into stable drain batches.
pub(in crate::driver::schedule::ready) struct Set {
    store: Store,
}

impl Set {
    pub(in crate::driver::schedule::ready) fn try_new(
        capacity: settings::Ready,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            store: Store::try_new(capacity)?,
        })
    }

    pub(in crate::driver::schedule::ready) fn is_empty(&self) -> bool {
        use bitmap::Access as _;

        self.store.is_empty(Side::A) && self.store.is_empty(Side::B)
    }

    pub(in crate::driver::schedule::ready) fn drain(&self) -> Option<Drain<'_>> {
        Some(Drain::new(self, self.start()?))
    }

    pub(in crate::driver::schedule::ready) fn insert(&self, index: ready::DynamicIndex) {
        self.store.insert(self.store.state().active(), index)
    }

    pub(in crate::driver::schedule::ready) fn remove(&self, index: ready::DynamicIndex) -> Removal {
        self.store.remove(index, Removal::Removed, Removal::Absent)
    }

    pub(in crate::driver::schedule::ready) fn clear(&self, index: ready::DynamicIndex) {
        self.store.remove(index, (), ())
    }

    fn start(&self) -> Option<Side> {
        use bitmap::Access as _;

        let state = self.store.state();
        let side = match state.phase() {
            Phase::Draining => None,
            Phase::Paused(side) => Some(if self.store.is_empty(side) {
                state.active()
            } else {
                side
            }),
            Phase::Idle => Some(state.active()),
        }?;
        state.draining(side);
        Some(side)
    }

    fn pause(&self, side: Side) {
        use bitmap::Access as _;

        if self.store.is_empty(side) {
            self.seal(side);
        } else {
            self.store.state().paused(side);
        }
    }

    fn seal(&self, side: Side) {
        use bitmap::Access as _;

        let pending = side.other();
        let state = self.store.state();
        if self.store.is_empty(pending) {
            state.idle(pending);
        } else {
            state.paused(pending);
        }
    }
}

impl Return for &Set {
    type Item = Side;

    fn return_item(self, side: Side) {
        self.pause(side);
    }
}

const _: () = assert!(std::mem::size_of::<Set>() == 2 * std::mem::size_of::<usize>());

const WORD_BITS: usize = usize::BITS as usize;
const ENTRIES_PER_WORD: usize = WORD_BITS / 2;
const LOW_MASK: usize = usize::MAX / 3;
const SIDE_MASKS: [usize; 2] = [LOW_MASK, LOW_MASK << 1];

struct Store {
    cells: array::Block<cell::Cell<usize>, Extent, (state::State, [cell::Cell<u16>; 2])>,
}

#[derive(Clone, Copy)]
struct Extent(settings::Ready);

impl Extent {
    const fn capacity(self) -> usize {
        self.0.raw() as usize
    }

    const fn base(self) -> usize {
        self.words().get() as usize
    }

    const fn words(self) -> num::NonZero<u32> {
        let count = self.0.into_nonzero().get() as u32;
        num::NonZero::<u32>::MIN.saturating_add((count - 1) / ENTRIES_PER_WORD as u32)
    }

    fn shape(self) -> bitmap::Shape<num::NonZero<u32>> {
        bitmap::Shape::<num::NonZero<u32>>::new(self.words())
    }

    fn summary(self) -> usize {
        self.shape().words()
    }
}

// SAFETY: the immutable ready capacity determines one exact length in every copy.
unsafe impl alloc::raw::Extent for Extent {
    fn len(&self) -> usize {
        self.base() + 2 * self.summary()
    }
}

// SAFETY: the immutable ready extent bounds both initialized summary planes.
// Only bitmap operations mutate them; base transitions maintain their exact occupancy.
// The u16 ready capacity has at least one and at most 2048 packed words, hence two
// summary levels in an extent whose bitmap domain is never empty.
unsafe impl depot::raw::Words for Store {
    type Plane = Side;
    type Size = num::NonZero<u32>;

    fn shape(&self) -> bitmap::Shape<num::NonZero<u32>> {
        self.cells.extent().shape()
    }

    fn words(&self, side: Side) -> &[bitmap::Word] {
        let extent = self.cells.extent();
        let start = extent.base() + side.index() * extent.summary();
        let cells: &[cell::Cell<usize>] = &self.cells;
        // SAFETY: Extent allocates the complete exact summary after the packed base.
        bitmap::Word::from_cells(unsafe { cells.get_unchecked(start..start + extent.summary()) })
    }
}

impl Store {
    fn try_new(capacity: settings::Ready) -> Result<Self, Error> {
        let extent = Extent(capacity);
        let state = (state::State::new(), [const { cell::Cell::new(0) }; 2]);
        let cells = array::Block::try_new(extent, state)?.repeat_with(cell::Cell::default);
        Ok(Self { cells })
    }

    fn state(&self) -> &state::State {
        &self.cells.state().0
    }

    /// The packed position of one ready code, admitted once into this extent.
    ///
    /// The validated plan's nonempty descriptor table makes the packed domain
    /// nonempty, so the row carries the transition's word and both planes.
    fn at(&self, index: usize) -> entry::Row<&Store> {
        entry::Row::clamp(self, index / ENTRIES_PER_WORD)
    }

    /// Publishes one insertion or removal on the caller's exact side.
    /// Only that side can change occupancy; the other plane remains unchanged.
    fn publish<'s, const SET: bool>(
        &'s self,
        row: entry::Row<&'s Store>,
        side: Side,
        before: usize,
        after: usize,
    ) {
        // SAFETY: the row retains this exact packed position inside the extent.
        unsafe { self.base().get_unchecked(row.index()) }.set(after);
        let stable = if SET { before } else { after };
        if stable & SIDE_MASKS[side.index()] == 0 {
            row.update::<SET>(side);
        }
    }

    fn insert(&self, side: Side, index: ready::DynamicIndex) {
        let row = self.at(index.get());
        let shift = (index.get() % ENTRIES_PER_WORD) * 2;
        // SAFETY: the row retains this exact packed position inside the extent.
        let word = unsafe { self.base().get_unchecked(row.index()) };
        let current = word.get();
        if current & (3usize << shift) != 0 {
            return;
        }
        self.publish::<true>(
            row,
            side,
            current,
            current | (1usize << (shift + side.index())),
        );
    }

    /// Removes the sole side encoded by an insertion into an empty packed position.
    fn remove<R>(&self, index: ready::DynamicIndex, removed: R, absent: R) -> R {
        let row = self.at(index.get());
        let shift = (index.get() % ENTRIES_PER_WORD) * 2;
        // SAFETY: the row retains this exact packed position inside the extent.
        let word = unsafe { self.base().get_unchecked(row.index()) };
        let current = word.get();
        let bits = current & (3usize << shift);
        if bits == 0 {
            return absent;
        }
        let side = if bits & LOW_MASK != 0 {
            Side::A
        } else {
            Side::B
        };
        self.publish::<false>(row, side, current, current & !bits);
        removed
    }

    fn next(&self, side: Side) -> Option<ready::DynamicIndex> {
        let start = usize::from(self.cursor(side).get());
        match self.take(side, start) {
            None if start != 0 => self.take(side, 0),
            item => item,
        }
    }

    fn base(&self) -> &[cell::Cell<usize>] {
        let cells: &[cell::Cell<usize>] = &self.cells;
        unsafe { cells.get_unchecked(..self.cells.extent().base()) }
    }

    fn cursor(&self, side: Side) -> &cell::Cell<u16> {
        &self.cells.state().1[side.index()]
    }

    fn take(&self, side: Side, start: usize) -> Option<ready::DynamicIndex> {
        if start >= self.cells.extent().capacity() {
            return None;
        }
        let shift = (start % ENTRIES_PER_WORD) * 2 + side.index();
        // SAFETY: start is below this exact immutable capacity, so its packed
        // row lies in this store and its one-based code fits u32.
        let position =
            unsafe { num::NonZero::<u32>::new_unchecked((start / ENTRIES_PER_WORD) as u32 + 1) };
        // SAFETY: the bound above and retained store borrow admit this exact row.
        let first = entry::Row::from_source(unsafe { depot::raw::Proof::new((self, position)) });
        // SAFETY: the row retains this exact packed position inside the extent.
        let word = unsafe { self.base().get_unchecked(first.index()) };
        let current = word.get();
        let selected = current & SIDE_MASKS[side.index()] & (usize::MAX << shift);
        let (row, current, bits) = match num::NonZero::<usize>::new(selected) {
            Some(bits) => (first, current, bits),
            None => {
                let row = bitmap::Access::find(self, side, first.index() + 1)?;
                // SAFETY: the row retains this exact packed position inside the extent.
                let current = unsafe { self.base().get_unchecked(row.index()) }.get();
                // SAFETY: the found row proves this exact packed word holds a set
                // bit for `side`, so its side mask is nonzero.
                let bits =
                    unsafe { num::NonZero::new_unchecked(current & SIDE_MASKS[side.index()]) };
                (row, current, bits)
            }
        };
        let bit = bits.trailing_zeros() as usize;
        self.publish::<false>(row, side, current, current & !(1usize << bit));
        let index = row.index() * ENTRIES_PER_WORD + bit / 2;
        self.cursor(side)
            .set(if index + 1 == self.cells.extent().capacity() {
                0
            } else {
                index as u16 + 1
            });
        // SAFETY: the checked domain keeps the index below the extent capacity, so the increment cannot wrap.
        let code = unsafe { num::NonZero::<u16>::new_unchecked(index as u16 + 1) };
        Some(ready::DynamicIndex(code))
    }
}

const _: () = {
    assert!(WORD_BITS == 64);
    assert!(!mem::needs_drop::<cell::Cell<usize>>());
    assert!(mem::size_of::<Store>() == 2 * mem::size_of::<usize>());
};
