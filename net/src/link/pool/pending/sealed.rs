use std::{cell, marker, mem, process, rc};

use dope_core::driver::route::{self, table, table::entries::vacant};
use o3::collections;

use crate::link::{self, pool, pool::pending};

const NONE: u32 = 1 << route::SLOT_BITS;
const NEXT_BITS: u32 = route::SLOT_BITS + 1;
const NEXT_MASK: u32 = (1 << NEXT_BITS) - 1;
const ACTION_SHIFT: u32 = NEXT_BITS;
const ACTION_MASK: u32 = 0b111 << ACTION_SHIFT;
const QUEUED: u32 = 1 << (ACTION_SHIFT + 3);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct Word(u32);

#[derive(Clone, Copy)]
struct State {
    head: u32,
    tail: u32,
    len: u32,
}

struct Core {
    nodes: Box<[Node]>,
    state: cell::Cell<State>,
    _thread: marker::PhantomData<rc::Rc<()>>,
}

#[derive(Clone, Copy)]
struct Entry<'a> {
    core: &'a Core,
    index: route::SlotIndex,
    epoch: route::Epoch,
}

struct Node {
    word: cell::Cell<Word>,
    epoch: cell::Cell<Option<route::Epoch>>,
}

pub(in crate::link::pool) struct Queue<'d, const ID: u8> {
    core: Core,
    route: marker::PhantomData<fn(&'d route::KeyTag<ID>) -> &'d route::KeyTag<ID>>,
}

pub(in crate::link) struct Vacancy<'a, 'd, const ID: u8, T> {
    reservation: vacant::Entry<'a, T, route::KeyTag<ID>>,
    key: pool::Key<'d, ID>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Handle<'a> {
    entry: Entry<'a>,
}

impl Word {
    const EMPTY: Self = Self(NONE);

    fn queued(self) -> bool {
        self.0 & QUEUED != 0
    }

    fn next(self) -> u32 {
        self.0 & NEXT_MASK
    }

    fn with_next(self, next: u32) -> Self {
        debug_assert!(next <= NONE);
        Self((self.0 & !NEXT_MASK) | next)
    }

    fn with_action(self, action: pending::Action) -> Self {
        Self(self.0 | ((u32::from(action.bit())) << ACTION_SHIFT))
    }

    fn schedule(action: pending::Action) -> Self {
        Self::EMPTY.with_action(action).with_queued()
    }

    fn with_queued(self) -> Self {
        Self(self.0 | QUEUED)
    }

    fn clear_actions(self) -> Self {
        Self(self.0 & !ACTION_MASK)
    }

    fn actions(self) -> u8 {
        ((self.0 & ACTION_MASK) >> ACTION_SHIFT) as u8
    }
}

impl State {
    const EMPTY: Self = Self {
        head: NONE,
        tail: NONE,
        len: 0,
    };
}

const _: () = assert!(mem::size_of::<Word>() == mem::size_of::<u32>());
const _: () = assert!(mem::size_of::<State>() == mem::size_of::<[u32; 3]>());
const _: () = assert!(mem::size_of::<Node>() == 2 * mem::size_of::<u64>());
const _: () = assert!(mem::size_of::<Queue<'static, 0>>() == mem::size_of::<Core>());

impl<'d, const ID: u8> Queue<'d, ID> {
    pub(in crate::link::pool) fn try_with_capacity(
        capacity: table::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        let capacity = capacity.get();
        Ok(Self {
            core: Core {
                nodes: collections::BoxSliceExt::try_box_with(capacity, |_| Node {
                    word: cell::Cell::new(Word::EMPTY),
                    epoch: cell::Cell::new(None),
                })?,
                state: cell::Cell::new(State::EMPTY),
                _thread: marker::PhantomData,
            },
            route: marker::PhantomData,
        })
    }

    fn bind(&self, key: pool::Key<'d, ID>) -> Option<Entry<'_>> {
        self.core.bind(key.lane(), key.epoch())
    }

    pub(in crate::link::pool) fn handle(&self, key: pool::Key<'d, ID>) -> Option<Handle<'_>> {
        self.core.entry(key.lane(), key.epoch()).map(Handle::new)
    }

    pub(super) fn pop(
        &self,
        keys: pool::Keyspace<'d, ID>,
    ) -> Option<(pool::Key<'d, ID>, pending::Work)> {
        let (index, epoch, work) = self.core.pop()?;
        Some((keys.bind_parts(index, epoch), work))
    }

    pub(super) fn len(&self) -> usize {
        self.core.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.core.is_empty()
    }
}

impl<'a, 'd, const ID: u8, T> Vacancy<'a, 'd, ID, T> {
    pub(in crate::link::pool) fn new(
        pending: &'a Queue<'d, ID>,
        rearm: &link::Rearm<'d, ID>,
        keys: pool::Keyspace<'d, ID>,
        reservation: vacant::Entry<'a, T, route::KeyTag<ID>>,
    ) -> Self {
        let key = rearm.key(keys, &reservation);
        let Some(_) = pending.bind(key) else {
            process::abort();
        };
        Self { reservation, key }
    }

    pub(in crate::link::pool) fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }

    pub(in crate::link) fn index(&self) -> route::SlotIndex {
        self.key.lane()
    }

    pub(in crate::link::pool) fn commit_with<R>(
        self,
        build: impl FnOnce(pool::Key<'d, ID>) -> (T, R),
    ) -> R {
        let Self { reservation, key } = self;
        let (value, result) = build(key);
        reservation.insert(value);
        result
    }
}

impl Core {
    fn bind(&self, index: route::SlotIndex, epoch: route::Epoch) -> Option<Entry<'_>> {
        let node = self.nodes.get(index.raw() as usize)?;
        node.word.set(node.word.get().clear_actions());
        node.epoch.set(Some(epoch));
        Some(Entry {
            core: self,
            index,
            epoch,
        })
    }

    fn entry(&self, index: route::SlotIndex, epoch: route::Epoch) -> Option<Entry<'_>> {
        let node = self.nodes.get(index.raw() as usize)?;
        (node.epoch.get() == Some(epoch)).then_some(Entry {
            core: self,
            index,
            epoch,
        })
    }

    fn linked_entry(&self, raw: u32) -> Entry<'_> {
        let Some(index) = route::SlotIndex::try_new(raw) else {
            process::abort();
        };
        let Some(node) = self.nodes.get(index.raw() as usize) else {
            process::abort();
        };
        let Some(epoch) = node.epoch.get() else {
            process::abort();
        };
        let Some(entry) = self.entry(index, epoch) else {
            process::abort();
        };
        entry
    }

    fn mark(&self, entry: Entry<'_>, action: pending::Action) {
        let node = entry.node();
        if node.epoch.get() != Some(entry.epoch) {
            return;
        }
        let word = node.word.get();
        if word.queued() {
            node.word.set(word.with_action(action));
            return;
        }
        debug_assert!(word == Word::EMPTY);

        let mut state = self.state.get();
        if state.tail == NONE {
            state.head = entry.index.raw();
        } else {
            let tail = self.linked_entry(state.tail).node();
            let tail_word = tail.word.get();
            debug_assert!(tail_word.queued());
            debug_assert_eq!(tail_word.next(), NONE);
            tail.word.set(tail_word.with_next(entry.index.raw()));
        }
        node.word.set(Word::schedule(action));
        state.tail = entry.index.raw();
        state.len += 1;
        self.state.set(state);
    }

    fn pop(&self) -> Option<(route::SlotIndex, route::Epoch, pending::Work)> {
        let mut state = self.state.get();
        if state.head == NONE {
            return None;
        }
        let entry = self.linked_entry(state.head);
        let word = entry.node().word.replace(Word::EMPTY);
        if !word.queued() {
            process::abort();
        }
        state.head = word.next();
        state.len -= 1;
        if state.head == NONE {
            state.tail = NONE;
        }
        self.state.set(state);
        Some((entry.index, entry.epoch, pending::Work(word.actions())))
    }

    fn len(&self) -> usize {
        self.state.get().len as usize
    }

    fn is_empty(&self) -> bool {
        self.state.get().head == NONE
    }
}

impl<'a> Entry<'a> {
    fn node(self) -> &'a Node {
        // SAFETY: `Entry` is constructed only after checking this exact fixed
        // node slice, and its borrow prevents the allocation from being dropped.
        unsafe { self.core.nodes.get_unchecked(self.index.raw() as usize) }
    }

    fn mark(self, action: pending::Action) {
        self.core.mark(self, action);
    }

    fn contains(self, action: pending::Action) -> bool {
        let node = self.node();
        node.epoch.get() == Some(self.epoch) && node.word.get().actions() & action.bit() != 0
    }
}

impl<'a> Handle<'a> {
    const fn new(entry: Entry<'a>) -> Self {
        Self { entry }
    }

    pub fn mark(self, action: pending::Action) {
        self.entry.mark(action);
    }

    pub fn contains(self, action: pending::Action) -> bool {
        self.entry.contains(action)
    }
}
