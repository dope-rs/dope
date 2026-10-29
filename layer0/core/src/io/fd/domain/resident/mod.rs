use std::{cell, mem};

use o3::{
    alloc,
    base::{marker::ThreadBound, num},
};

use crate::{
    driver::{
        route,
        route::{indices, table},
    },
    io::fd::{domain::Index, handles, states},
};

pub(super) mod retire;

const STATE_BITS: u32 = 3;
const PHASE_BITS: u32 = 2;
const PHASE_VALUE_MASK: u64 = (1_u64 << PHASE_BITS) - 1;
const PHASE_MASK: u64 = PHASE_VALUE_MASK << route::AUX_SHIFT;
const LEASE_VALUE: u8 = 1 << PHASE_BITS;
const LEASE_MASK: u64 = (LEASE_VALUE as u64) << route::AUX_SHIFT;
const STATE_VALUE_MASK: u64 = (1_u64 << STATE_BITS) - 1;
const STATE_MASK: u64 = STATE_VALUE_MASK << route::AUX_SHIFT;
const LIFECYCLE_SHIFT: u32 = route::AUX_SHIFT + STATE_BITS;
const LIFECYCLE_MASK: u64 = (u16::MAX as u64) << LIFECYCLE_SHIFT;
const EXHAUSTED_SHIFT: u32 = LIFECYCLE_SHIFT + u16::BITS;
const EXHAUSTED_MASK: u64 = 1_u64 << EXHAUSTED_SHIFT;
const INFLIGHT_MASK: u64 = EXHAUSTED_MASK << 1;
const SIDE_MASK: u64 = INFLIGHT_MASK << 1;
const BOUND_MASK: u64 = SIDE_MASK << 1;
const RETIREMENT_MASK: u64 = INFLIGHT_MASK | SIDE_MASK | BOUND_MASK;
const TARGET_WORDS: usize = 2;

const LINK_BITS: u32 = route::SLOT_BITS + 1;
const LINK_MASK: u64 = (1_u64 << LINK_BITS) - 1;
const EMPTY_LINK: u32 = 1_u32 << route::SLOT_BITS;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::io::fd::domain) enum Phase {
    Ready,
    Pending,
    InFlight,
    Quiescent,
}

pub(super) struct Store {
    storage: Box<[cell::Cell<u64>]>,
    pending: Queue,
    inflight: cell::Cell<Inflight>,
    thread: ThreadBound,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct Inflight(u32);

struct Entry<'a> {
    words: Words<'a>,
    generation: Counter<'a>,
}

pub(super) struct Lifecycle<'a> {
    meta: &'a cell::Cell<u64>,
}

struct Queue {
    state: cell::Cell<QueueState>,
}

#[derive(Clone, Copy)]
struct QueueState {
    heads: [Option<indices::SlotIndex>; 2],
    active: Side,
    draining: Option<Side>,
}

#[derive(Clone, Copy)]
pub(super) enum Side {
    Zero,
    One,
}

#[derive(Clone, Copy)]
struct Counter<'a> {
    word: &'a cell::Cell<u64>,
}

impl Phase {
    /// The native table retains a quarantined descriptor without a host lease.
    const QUARANTINED: Self = Self::Quiescent;

    const fn can_claim(self, lease: bool, spent: bool) -> bool {
        !lease && !spent && matches!(self, Self::Ready | Self::InFlight)
    }

    const fn is_inflight(self) -> bool {
        matches!(self, Self::InFlight)
    }

    fn from_bits(low: bool, high: bool) -> Self {
        match (low, high) {
            (false, false) => Self::Ready,
            (true, false) => Self::Pending,
            (false, true) => Self::InFlight,
            (true, true) => Self::Quiescent,
        }
    }
}

impl Store {
    pub(super) fn try_new(capacity: table::Capacity) -> Result<Self, alloc::Error> {
        use alloc::BoxSliceExt;

        let capacity = capacity.raw() as usize;
        let words = capacity * (TARGET_WORDS + 1);
        let storage = BoxSliceExt::try_default(words)?;
        Ok(Self {
            storage,
            pending: Queue::new(),
            inflight: cell::Cell::new(Inflight::EMPTY),
            thread: ThreadBound::NEW,
        })
    }

    pub(super) fn claim(&self, index: Index, state: states::SlotState) -> num::Generation {
        let entry = self.bind(Bound::mapped(index));
        entry.claim();
        Lifecycle {
            meta: entry.words.meta(),
        }
        .set(state);
        entry.generation.get()
    }

    pub(super) fn try_claim(&self, index: indices::SlotIndex) -> Option<num::Generation> {
        let entry = self.entry(index)?;
        if !entry
            .phase()
            .can_claim(entry.has_lease(), entry.is_exhausted())
        {
            return None;
        }
        entry.claim();
        Some(entry.generation.get())
    }

    pub(super) const fn retire(&self) -> retire::View<'_> {
        retire::View::new(self)
    }

    pub(super) fn set_target(
        &self,
        index: indices::SlotIndex,
        version: num::Generation,
        target: route::Token,
    ) {
        if let Some(entry) = self.resolve(index, version) {
            entry.set_target(target);
        }
    }

    pub(super) fn target(
        &self,
        index: indices::SlotIndex,
        version: num::Generation,
    ) -> Option<route::Token> {
        let entry = self.entry(index)?;
        if entry.generation.get() != version {
            return None;
        }
        entry.dispatch()
    }

    pub(super) fn target_at(&self, index: indices::SlotIndex) -> Option<route::Token> {
        let entry = self.entry(index)?;
        entry.dispatch()
    }

    pub(super) fn lifecycle(&self, slot: handles::PhysicalSlot) -> Lifecycle<'_> {
        let entry = self.bind(Bound::slot(slot));
        Lifecycle {
            meta: entry.words.meta(),
        }
    }

    pub(super) fn mapped(&self, index: Index) -> Lifecycle<'_> {
        let entry = self.bind(Bound::mapped(index));
        Lifecycle {
            meta: entry.words.meta(),
        }
    }

    pub(super) fn has_retiring(&self) -> bool {
        let solo = u64::from(states::SlotState::STANDALONE.raw()) << LIFECYCLE_SHIFT;
        self.storage
            .as_chunks::<TARGET_WORDS>()
            .0
            .iter()
            .take(self.capacity())
            .any(|words| {
                let state = words[1].get() & (STATE_MASK | LIFECYCLE_MASK | EXHAUSTED_MASK);
                state == solo | ((Phase::Pending as u64) << route::AUX_SHIFT)
                    || state == solo | ((Phase::InFlight as u64) << route::AUX_SHIFT)
            })
    }

    fn resolve(&self, index: indices::SlotIndex, version: num::Generation) -> Option<Entry<'_>> {
        let entry = self.entry(index)?;
        (entry.generation.get() == version && entry.phase() == Phase::Ready && entry.has_lease())
            .then_some(entry)
    }

    fn entry(&self, index: indices::SlotIndex) -> Option<Entry<'_>> {
        let bound = Bound::try_new(index, self.capacity())?;
        Some(self.bind(bound))
    }

    fn bind(&self, bound: Bound) -> Entry<'_> {
        let capacity = self.capacity();
        let [meta, data, word] = bound.cells(&self.storage, capacity);
        Entry {
            words: Words::new(meta, data, self.thread),
            generation: Counter { word },
        }
    }

    fn capacity(&self) -> usize {
        self.storage.len() / (TARGET_WORDS + 1)
    }
}

impl Entry<'_> {
    fn phase(&self) -> Phase {
        let raw = ((self.words.meta().get() & PHASE_MASK) >> route::AUX_SHIFT) as u8;
        Phase::from_bits(raw & 1 != 0, raw & 2 != 0)
    }

    fn has_lease(&self) -> bool {
        self.words.meta().get() & LEASE_MASK != 0
    }

    fn set_phase(&self, phase: Phase) {
        let meta = self.words.meta().get() & !PHASE_MASK;
        self.words
            .meta()
            .set(meta | (u64::from(phase as u8) << route::AUX_SHIFT));
    }

    fn set_status(&self, phase: Phase, lease: bool) {
        let meta = self.words.meta().get() & !STATE_MASK;
        let state = u64::from(phase as u8) | (u64::from(lease) << PHASE_BITS);
        self.words.meta().set(meta | (state << route::AUX_SHIFT));
    }

    fn is_exhausted(&self) -> bool {
        self.words.meta().get() & EXHAUSTED_MASK != 0
    }

    fn advance(&self) {
        match self.generation.get().next() {
            Some(next) => self.generation.set(next),
            None => self
                .words
                .meta()
                .set(self.words.meta().get() | EXHAUSTED_MASK),
        }
    }

    fn retired(&self) -> Option<num::Generation> {
        if self.is_exhausted() {
            Some(self.generation.get())
        } else {
            Some(num::Generation::from_raw(
                self.generation.get().get().checked_sub(1)?,
            ))
        }
    }

    fn recycle(&self) {
        self.set_status(Phase::Ready, false);
    }

    fn claim(&self) {
        self.words.identity().set(0);
        self.set_status(Phase::Ready, true);
    }

    fn set_target(&self, target: route::Token) {
        self.words
            .publish(target, STATE_VALUE_MASK as u8, LEASE_VALUE);
    }

    fn dispatch(&self) -> Option<route::Token> {
        self.words.dispatch(STATE_VALUE_MASK as u8, LEASE_VALUE)
    }
}

impl Inflight {
    const EMPTY: Self = Self(0);

    fn submit(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    fn finish(self) -> Self {
        Self(self.0.wrapping_sub(1))
    }

    const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl Queue {
    const fn new() -> Self {
        Self {
            state: cell::Cell::new(QueueState {
                heads: [None, None],
                active: Side::Zero,
                draining: None,
            }),
        }
    }

    fn push(&self, index: indices::SlotIndex, link: &cell::Cell<u64>) {
        let mut state = self.state.get();
        let side = state.active;
        let head = state.heads[side.index()];
        link.set(u64::from(head.map_or(EMPTY_LINK, indices::SlotIndex::raw)));
        state.heads[side.index()] = Some(index);
        self.state.set(state);
    }

    fn front(&self) -> Option<(indices::SlotIndex, Side)> {
        let mut state = self.state.get();
        let side = state.draining.unwrap_or(state.active);
        let head = state.heads[side.index()]?;
        if state.draining.is_none() {
            state.active = side.other();
            state.draining = Some(side);
            self.state.set(state);
        }
        Some((head, side))
    }

    fn commit(&self, side: Side, next: u32) {
        let mut state = self.state.get();
        let head = if next == EMPTY_LINK {
            state.draining = None;
            None
        } else {
            Some(indices::SlotIndex::from_field(u64::from(next)))
        };
        state.heads[side.index()] = head;
        self.state.set(state);
    }
}

impl Side {
    const fn index(self) -> usize {
        match self {
            Self::Zero => 0,
            Self::One => 1,
        }
    }

    const fn other(self) -> Self {
        match self {
            Self::Zero => Self::One,
            Self::One => Self::Zero,
        }
    }
}

impl Lifecycle<'_> {
    pub(super) fn is_exhausted(&self) -> bool {
        self.meta.get() & EXHAUSTED_MASK != 0
    }

    pub(super) fn get(&self) -> states::SlotState {
        let raw = (self.meta.get() & LIFECYCLE_MASK) >> LIFECYCLE_SHIFT;
        states::SlotState::from_raw(raw as u16)
    }

    pub(super) fn set(&self, state: states::SlotState) {
        let meta = self.meta.get() & !LIFECYCLE_MASK;
        self.meta
            .set(meta | (u64::from(state.raw()) << LIFECYCLE_SHIFT));
    }
}

impl Counter<'_> {
    fn get(self) -> num::Generation {
        num::Generation::from_raw(self.word.get() as u32)
    }

    fn set(self, value: num::Generation) {
        let word = self.word.get() & !u64::from(u32::MAX);
        self.word.set(word | u64::from(value.get()));
    }

    fn submitted(self) -> num::Generation {
        num::Generation::from_raw((self.word.get() >> u32::BITS) as u32)
    }

    fn submit(self, value: num::Generation) {
        let word = self.word.get() & u64::from(u32::MAX);
        self.word.set(word | (u64::from(value.get()) << u32::BITS));
    }
}

const _: () = {
    let phases = [
        Phase::Ready,
        Phase::Pending,
        Phase::InFlight,
        Phase::Quiescent,
    ];
    let mut index = 0;
    while index < phases.len() {
        assert!(!phases[index].can_claim(false, true));
        assert!(!phases[index].can_claim(true, false));
        assert!(!phases[index].can_claim(true, true));
        index += 1;
    }
    assert!(!Phase::QUARANTINED.can_claim(false, false));
    assert!(!Phase::QUARANTINED.is_inflight());
    assert!(Phase::Ready.can_claim(false, false));
    assert!(Phase::InFlight.can_claim(false, false));
    assert!(BOUND_MASK < 1_u64 << (route::AUX_SHIFT + route::AUX_BITS));
    assert!(EMPTY_LINK as u64 == route::SLOT_MASK + 1);
    assert!(mem::size_of::<Store>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Entry<'static>>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Lifecycle<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Counter<'static>>() == mem::size_of::<usize>());
};

mod words;
pub(super) use words::{Bound, Words};
