use std::mem::{MaybeUninit, replace};

use super::FixedMap;
use crate::driver::token::Token;
use crate::io::fd::FdSlot;
use crate::io::provided::raw::buffer::BufferId;
use libc::ECANCELED;

const NONE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
pub(crate) enum PendingCompletion {
    Accept {
        ud: Token,
        result: i32,
        more: bool,
    },
    Recv {
        ud: Token,
        result: i32,
        more: bool,
        bid: Option<BufferId>,
    },
    Write {
        ud: Token,
        result: i32,
    },
    Create {
        ud: Token,
        result: i32,
        slot: Option<FdSlot>,
    },
    Timer {
        ud: Token,
    },
    Shutdown,
}

struct PendingNode {
    value: MaybeUninit<PendingCompletion>,
    prev: u32,
    next: u32,
    create_prev: u32,
    create_next: u32,
    target_prev: u32,
    target_next: u32,
}

pub(crate) struct PendingQueue {
    nodes: Box<[PendingNode]>,
    create_heads: Box<[u32]>,
    target_heads: FixedMap<u32>,
    free: u32,
    head: u32,
    tail: u32,
    len: usize,
}

pub(crate) struct Extracted {
    head: u32,
}

impl PendingQueue {
    pub(super) fn with_capacity(capacity: usize, create_slots: usize) -> Self {
        assert!(capacity < NONE as usize);
        let mut nodes = Vec::with_capacity(capacity);
        for index in 0..capacity {
            nodes.push(PendingNode {
                value: MaybeUninit::uninit(),
                prev: NONE,
                next: if index + 1 == capacity {
                    NONE
                } else {
                    (index + 1) as u32
                },
                create_prev: NONE,
                create_next: NONE,
                target_prev: NONE,
                target_next: NONE,
            });
        }
        Self {
            nodes: nodes.into_boxed_slice(),
            create_heads: vec![NONE; create_slots].into_boxed_slice(),
            target_heads: FixedMap::with_capacity(capacity),
            free: if capacity == 0 { NONE } else { 0 },
            head: NONE,
            tail: NONE,
            len: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
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

    pub(super) fn push_back(&mut self, value: PendingCompletion) -> bool {
        let create_slot = match value {
            PendingCompletion::Create { slot, .. } => slot.map(|slot| slot.raw() as usize),
            _ => None,
        };
        let target = value.token().map(|token| token.raw() as usize);
        if self.free == NONE || create_slot.is_some_and(|slot| slot >= self.create_heads.len()) {
            return false;
        }

        let index = self.free;
        let node = &mut self.nodes[index as usize];
        self.free = node.next;
        node.value.write(value);
        node.prev = self.tail;
        node.next = NONE;
        node.create_prev = NONE;
        node.create_next = NONE;
        node.target_prev = NONE;
        node.target_next = NONE;

        if self.tail == NONE {
            self.head = index;
        } else {
            self.nodes[self.tail as usize].next = index;
        }
        self.tail = index;

        if let Some(slot) = create_slot {
            let create_head = self.create_heads[slot];
            self.nodes[index as usize].create_next = create_head;
            if create_head != NONE {
                self.nodes[create_head as usize].create_prev = index;
            }
            self.create_heads[slot] = index;
        }

        if let Some(target) = target {
            let target_head = self.target_heads.get(&target).copied().unwrap_or(NONE);
            self.nodes[index as usize].target_next = target_head;
            if target_head != NONE {
                self.nodes[target_head as usize].target_prev = index;
            }
            assert!(self.target_heads.insert(target, index).is_ok());
        }

        self.len += 1;
        true
    }

    pub(crate) fn pop_front(&mut self) -> Option<PendingCompletion> {
        let index = self.head;
        if index == NONE {
            return None;
        }
        let value = unsafe { self.nodes[index as usize].value.assume_init_read() };
        self.unlink(index);
        if let PendingCompletion::Create {
            slot: Some(slot), ..
        } = value
        {
            self.unlink_create(index, slot.raw() as usize);
        }
        if let Some(target) = value.token() {
            self.unlink_target(index, target.raw() as usize);
        }
        self.release(index);
        Some(value)
    }

    pub(crate) fn cancel_create(&mut self, slot: FdSlot) -> usize {
        let Some(head) = self.create_heads.get_mut(slot.raw() as usize) else {
            return 0;
        };
        let mut index = replace(head, NONE);
        let mut cancelled = 0;
        while index != NONE {
            let node = &mut self.nodes[index as usize];
            let next = node.create_next;
            let PendingCompletion::Create { result, slot, .. } =
                (unsafe { node.value.assume_init_mut() })
            else {
                unreachable!()
            };
            *result = -ECANCELED;
            *slot = None;
            node.create_prev = NONE;
            node.create_next = NONE;
            index = next;
            cancelled += 1;
        }
        cancelled
    }

    pub(crate) fn extract_targets(&mut self, targets: &[Token]) -> Extracted {
        let mut head = NONE;
        let mut tail = NONE;
        for target in targets {
            let mut index = self
                .target_heads
                .remove(&(target.raw() as usize))
                .unwrap_or(NONE);
            while index != NONE {
                let target_next = self.nodes[index as usize].target_next;
                let value = unsafe { *self.nodes[index as usize].value.assume_init_ref() };
                self.unlink(index);
                if let PendingCompletion::Create {
                    slot: Some(slot), ..
                } = value
                {
                    self.unlink_create(index, slot.raw() as usize);
                }
                self.nodes[index as usize].target_prev = NONE;
                self.nodes[index as usize].target_next = NONE;
                self.nodes[index as usize].next = NONE;
                if tail == NONE {
                    head = index;
                } else {
                    self.nodes[tail as usize].next = index;
                }
                tail = index;
                index = target_next;
            }
        }
        Extracted { head }
    }

    pub(crate) fn pop_extracted(&mut self, extracted: &mut Extracted) -> Option<PendingCompletion> {
        let index = extracted.head;
        if index == NONE {
            return None;
        }
        let node = &mut self.nodes[index as usize];
        extracted.head = node.next;
        let value = unsafe { node.value.assume_init_read() };
        self.release(index);
        Some(value)
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

    fn unlink_create(&mut self, index: u32, slot: usize) {
        let node = &self.nodes[index as usize];
        let prev = node.create_prev;
        let next = node.create_next;
        if prev == NONE {
            self.create_heads[slot] = next;
        } else {
            self.nodes[prev as usize].create_next = next;
        }
        if next != NONE {
            self.nodes[next as usize].create_prev = prev;
        }
    }

    fn unlink_target(&mut self, index: u32, target: usize) {
        let node = &self.nodes[index as usize];
        let prev = node.target_prev;
        let next = node.target_next;
        if prev == NONE {
            if next == NONE {
                self.target_heads.remove(&target);
            } else {
                assert!(self.target_heads.insert(target, next).is_ok());
            }
        } else {
            self.nodes[prev as usize].target_next = next;
        }
        if next != NONE {
            self.nodes[next as usize].target_prev = prev;
        }
        let node = &mut self.nodes[index as usize];
        node.target_prev = NONE;
        node.target_next = NONE;
    }

    fn release(&mut self, index: u32) {
        let node = &mut self.nodes[index as usize];
        node.next = self.free;
        self.free = index;
    }
}

impl PendingCompletion {
    fn token(&self) -> Option<Token> {
        match self {
            Self::Accept { ud, .. }
            | Self::Recv { ud, .. }
            | Self::Write { ud, .. }
            | Self::Create { ud, .. }
            | Self::Timer { ud } => Some(*ud),
            Self::Shutdown => None,
        }
    }
}

impl Drop for PendingQueue {
    fn drop(&mut self) {
        let mut index = self.head;
        while index != NONE {
            let node = &mut self.nodes[index as usize];
            index = node.next;
            unsafe { node.value.assume_init_drop() };
        }
    }
}
