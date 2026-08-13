use std::{io, mem, process};

use o3::collections;

use crate::{
    backend::kqueue::engine::{event, table},
    driver::{flight, settings},
    io::fd::handles,
};

const NONE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
/// An index admitted only to the intrusive chain of create completions.
struct CreateIndex(u32);

impl CreateIndex {
    const NONE: Self = Self(NONE);

    fn new(index: u32) -> Self {
        debug_assert_ne!(index, NONE);
        Self(index)
    }

    const fn node(self) -> Option<usize> {
        if self.0 == NONE {
            None
        } else {
            Some(self.0 as usize)
        }
    }
}

struct PendingNode {
    value: mem::MaybeUninit<event::Completion>,
    prev: u32,
    next: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Delivery {
    Emit,
    Reclaim,
}

#[derive(Clone, Copy)]
struct TargetState {
    pending: u32,
    delivery: Delivery,
}

impl TargetState {
    const fn new() -> Self {
        Self {
            pending: 1,
            delivery: Delivery::Emit,
        }
    }

    fn increment(&mut self) {
        let Some(pending) = self.pending.checked_add(1) else {
            process::abort();
        };
        self.pending = pending;
    }
}

const _: () = assert!(mem::size_of::<CreateIndex>() == mem::size_of::<u32>());
const _: () = assert!(
    mem::size_of::<PendingNode>()
        == mem::size_of::<event::Completion>() + 2 * mem::size_of::<u32>()
);
const _: () = assert!(mem::size_of::<TargetState>() == 2 * mem::size_of::<u32>());

/// Owns completion resources until event draining binds them to a driver lifetime.
pub(crate) struct Queue {
    nodes: Box<[PendingNode]>,
    create_heads: Box<[CreateIndex]>,
    target_states: table::raw::Map<TargetState>,
    free: u32,
    head: u32,
    tail: u32,
    len: usize,
}

impl PendingNode {
    fn cancel_create(&mut self) {
        if let event::Completion::Create { outcome, .. } = unsafe { self.value.assume_init_mut() } {
            let Some(slot) = outcome.slot() else {
                return;
            };
            *outcome = event::CreateOutcome::Cancelled { slot };
        }
    }
}

impl Queue {
    pub(in crate::backend::kqueue) fn try_with_capacity(
        queues: settings::QueueLayout,
        create_slots: usize,
    ) -> io::Result<Self> {
        let capacity = queues.completions() as usize;
        debug_assert!(capacity < NONE as usize);
        let target_states = table::raw::Map::try_with_capacity(capacity)?;
        let nodes = collections::BoxSliceExt::try_box_with(capacity, |index| PendingNode {
            value: mem::MaybeUninit::uninit(),
            prev: NONE,
            next: if index + 1 == capacity {
                NONE
            } else {
                (index + 1) as u32
            },
        })?;
        let create_heads =
            collections::BoxSliceExt::try_box_with(create_slots, |_| CreateIndex::NONE)?;
        Ok(Self {
            nodes,
            create_heads,
            target_states,
            free: 0,
            head: NONE,
            tail: NONE,
            len: 0,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn is_full(&self) -> bool {
        self.free == NONE
    }

    pub(crate) fn remaining_capacity(&self) -> usize {
        self.nodes.len() - self.len
    }

    pub(crate) fn has_create(&self, slot: handles::FixedSlot) -> bool {
        self.create_heads
            .get(slot.raw() as usize)
            .is_some_and(|head| *head != CreateIndex::NONE)
    }

    pub(crate) fn push_back(&mut self, value: event::Completion) -> bool {
        let create_slot = match &value {
            event::Completion::Create { outcome, .. } => {
                outcome.slot().map(|slot| slot.raw() as usize)
            }
            _ => None,
        };
        let target = value.token().map(|token| token.raw() as usize);
        if self.free == NONE
            || create_slot.is_some_and(|slot| {
                slot >= self.create_heads.len() || self.create_heads[slot] != CreateIndex::NONE
            })
        {
            return false;
        }

        let index = self.free;
        let node = &mut self.nodes[index as usize];
        self.free = node.next;
        node.value.write(value);
        node.prev = self.tail;
        node.next = NONE;

        if self.tail == NONE {
            self.head = index;
        } else {
            self.nodes[self.tail as usize].next = index;
        }
        self.tail = index;

        if let Some(slot) = create_slot {
            let index = CreateIndex::new(index);
            debug_assert_eq!(self.create_heads[slot], CreateIndex::NONE);
            self.create_heads[slot] = index;
        }

        if let Some(target) = target {
            unsafe {
                self.target_states.upsert_unchecked(
                    target,
                    TargetState::new(),
                    TargetState::increment,
                );
            }
        }

        self.len += 1;
        true
    }

    pub(crate) fn pop_front(&mut self) -> Option<event::Dequeued> {
        let index = self.head;
        if index == NONE {
            return None;
        }
        let value = unsafe { self.nodes[index as usize].value.assume_init_read() };
        self.unlink(index);
        if let event::Completion::Create { outcome, .. } = &value
            && let Some(slot) = outcome.slot()
        {
            self.unlink_create(CreateIndex::new(index), slot.raw() as usize);
        }
        let delivery = value
            .token()
            .map_or(Delivery::Emit, |target| self.release_target(target));
        self.release(index);
        Some(match delivery {
            Delivery::Emit => event::Dequeued::Emit(value),
            Delivery::Reclaim => event::Dequeued::Reclaim(value),
        })
    }

    pub(in crate::backend::kqueue) fn pop_for_reclaim(&mut self) -> Option<event::Completion> {
        self.pop_front().map(event::Dequeued::into_completion)
    }

    pub(crate) fn cancel_create(&mut self, slot: handles::FixedSlot) {
        use std::mem::replace;
        let Some(head) = self.create_heads.get_mut(slot.raw() as usize) else {
            return;
        };
        let index = replace(head, CreateIndex::NONE);
        let Some(raw) = index.node() else {
            return;
        };
        self.nodes[raw].cancel_create();
    }

    pub(crate) fn suppress_target(&mut self, target: flight::raw::Echo) {
        let Some(state) = self.target_states.get_mut(&(target.raw() as usize)) else {
            return;
        };
        state.delivery = Delivery::Reclaim;
    }

    fn unlink(&mut self, index: u32) {
        let node = &self.nodes[index as usize];
        let prev = node.prev;
        let next = node.next;
        if prev == NONE {
            self.head = next;
        } else {
            self.nodes[prev as usize].next = next;
        }
        if next == NONE {
            self.tail = prev;
        } else {
            self.nodes[next as usize].prev = prev;
        }
        self.len -= 1;
    }

    fn unlink_create(&mut self, index: CreateIndex, slot: usize) {
        debug_assert_eq!(self.create_heads[slot], index);
        self.create_heads[slot] = CreateIndex::NONE;
    }

    fn release_target(&mut self, target: flight::raw::Echo) -> Delivery {
        let key = target.raw() as usize;
        let mut delivery = Delivery::Emit;
        unsafe {
            self.target_states.retain_unchecked(key, |state| {
                delivery = state.delivery;
                let Some(pending) = state.pending.checked_sub(1) else {
                    process::abort();
                };
                state.pending = pending;
                pending != 0
            });
        }
        delivery
    }

    fn release(&mut self, index: u32) {
        let node = &mut self.nodes[index as usize];
        node.next = self.free;
        self.free = index;
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        let mut index = self.head;
        while index != NONE {
            let node = &mut self.nodes[index as usize];
            index = node.next;
            unsafe { node.value.assume_init_drop() };
        }
    }
}
