use std::{cell, mem};

use o3::collections::{self, slab};

use crate::connector::attempt;

const NONE: u32 = u32::MAX;

struct Node<'d, const ID: u8> {
    key: cell::UnsafeCell<mem::MaybeUninit<attempt::Id<'d, ID>>>,
    prev: cell::Cell<u32>,
    next: cell::Cell<u32>,
}

impl<const ID: u8> Node<'_, ID> {
    fn vacant(index: u32) -> Self {
        Self {
            key: cell::UnsafeCell::new(mem::MaybeUninit::uninit()),
            prev: cell::Cell::new(index),
            next: cell::Cell::new(NONE),
        }
    }
}

#[derive(Clone, Copy)]
struct State {
    head: u32,
    tail: u32,
}

impl State {
    const EMPTY: Self = Self {
        head: NONE,
        tail: NONE,
    };
}

pub(crate) struct Pending<'d, const ID: u8> {
    nodes: Box<[Node<'d, ID>]>,
    dials: cell::Cell<State>,
    cancellations: cell::Cell<State>,
}

impl<'d, const ID: u8> Pending<'d, ID> {
    pub(super) fn try_with_capacity(
        capacity: slab::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        let capacity = capacity.get();
        let nodes =
            collections::BoxSliceExt::try_box_with(capacity, |index| Node::vacant(index as u32))?;
        Ok(Self {
            nodes,
            dials: cell::Cell::new(State::EMPTY),
            cancellations: cell::Cell::new(State::EMPTY),
        })
    }

    fn push_back(&self, queue: &cell::Cell<State>, key: attempt::Id<'d, ID>) {
        let index = key.index() as usize;
        debug_assert!(
            self.nodes
                .get(index)
                .is_some_and(|node| node.prev.get() == key.index())
        );
        let mut state = queue.get();
        // SAFETY: callers enqueue only an in-bounds node that is not linked.
        let node = unsafe { self.nodes.get_unchecked(index) };
        // SAFETY: an unlinked node's value is uninitialized.
        unsafe { (*node.key.get()).write(key) };
        node.prev.set(state.tail);
        node.next.set(NONE);
        if state.tail == NONE {
            state.head = key.index();
        } else {
            // SAFETY: a nonempty tail is the index of a linked node.
            unsafe { self.nodes.get_unchecked(state.tail as usize) }
                .next
                .set(key.index());
        }
        state.tail = key.index();
        queue.set(state);
    }

    fn push_front(&self, queue: &cell::Cell<State>, key: attempt::Id<'d, ID>) {
        let index = key.index() as usize;
        debug_assert!(
            self.nodes
                .get(index)
                .is_some_and(|node| node.prev.get() == key.index())
        );
        let mut state = queue.get();
        // SAFETY: callers enqueue only an in-bounds node that is not linked.
        let node = unsafe { self.nodes.get_unchecked(index) };
        // SAFETY: an unlinked node's value is uninitialized.
        unsafe { (*node.key.get()).write(key) };
        node.prev.set(NONE);
        node.next.set(state.head);
        if state.head == NONE {
            state.tail = key.index();
        } else {
            // SAFETY: a nonempty head is the index of a linked node.
            unsafe { self.nodes.get_unchecked(state.head as usize) }
                .prev
                .set(key.index());
        }
        state.head = key.index();
        queue.set(state);
    }

    fn pop_front(&self, queue: &cell::Cell<State>) -> Option<attempt::Id<'d, ID>> {
        let head = queue.get().head;
        if head == NONE {
            return None;
        }
        // SAFETY: a nonempty head is the index of a linked node.
        Some(unsafe { self.remove_unchecked(queue, head as usize) })
    }

    fn remove_if_queued(&self, queue: &cell::Cell<State>, key: attempt::Id<'d, ID>) {
        let index = key.index() as usize;
        debug_assert!(index < self.nodes.len());
        // SAFETY: key came from a slot removed from the capacity-matched slab.
        let node = unsafe { self.nodes.get_unchecked(index) };
        if node.prev.get() != key.index() {
            // SAFETY: a node whose prev is not its own index is linked.
            let removed = unsafe { self.remove_unchecked(queue, index) };
            debug_assert_eq!(removed, key);
        }
    }

    pub(super) fn dials_empty(&self) -> bool {
        self.dials.get().head == NONE
    }

    pub(super) fn push_dial(&self, key: attempt::Id<'d, ID>) {
        self.push_back(&self.dials, key);
    }

    pub(super) fn push_dial_front(&self, key: attempt::Id<'d, ID>) {
        self.push_front(&self.dials, key);
    }

    pub(super) fn pop_dial(&self) -> Option<attempt::Id<'d, ID>> {
        self.pop_front(&self.dials)
    }

    pub(super) fn remove_dial(&self, key: attempt::Id<'d, ID>) {
        self.remove_if_queued(&self.dials, key);
    }

    pub(super) fn push_cancellation(&self, key: attempt::Id<'d, ID>) {
        self.push_back(&self.cancellations, key);
    }

    pub(super) fn pop_cancellation(&self) -> Option<attempt::Id<'d, ID>> {
        self.pop_front(&self.cancellations)
    }

    pub(super) fn remove_cancellation(&self, key: attempt::Id<'d, ID>) {
        self.remove_if_queued(&self.cancellations, key);
    }

    unsafe fn remove_unchecked(
        &self,
        queue: &cell::Cell<State>,
        index: usize,
    ) -> attempt::Id<'d, ID> {
        debug_assert!(
            self.nodes
                .get(index)
                .is_some_and(|node| node.prev.get() != index as u32)
        );
        let mut state = queue.get();
        // SAFETY: the caller proves index names a linked node.
        let node = unsafe { self.nodes.get_unchecked(index) };
        let prev = node.prev.get();
        let next = node.next.get();
        node.prev.set(index as u32);
        node.next.set(NONE);
        if prev == NONE {
            state.head = next;
        } else {
            // SAFETY: prev names the preceding linked node.
            unsafe { self.nodes.get_unchecked(prev as usize) }
                .next
                .set(next);
        }
        if next == NONE {
            state.tail = prev;
        } else {
            // SAFETY: next names the following linked node.
            unsafe { self.nodes.get_unchecked(next as usize) }
                .prev
                .set(prev);
        }
        queue.set(state);
        // SAFETY: a linked node has an initialized key and unlinking transfers it out.
        unsafe { (*node.key.get()).assume_init_read() }
    }
}
