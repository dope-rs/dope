use std::cell::Cell;
use std::mem::replace;

use o3::cell::RawCell;
use o3::mem::{FairCreditLane, FairCreditPool, FairCreditState};

use super::super::super::config::Config;

const NONE: u32 = u32::MAX;

struct Node<T> {
    value: Option<T>,
    next: u32,
    bytes: usize,
    resident: usize,
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::link::egress) struct ReservedIndex(u32);

impl ReservedIndex {
    pub(in crate::link::egress) const NONE: Self = Self(NONE);

    pub(in crate::link::egress) fn is_none(self) -> bool {
        self == Self::NONE
    }

    pub(in crate::link::egress::metadata) fn into_linked(self) -> LinkedIndex {
        debug_assert!(!self.is_none());
        LinkedIndex(self.0)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::link::egress) struct LinkedIndex(u32);

impl LinkedIndex {
    pub(in crate::link::egress) const NONE: Self = Self(NONE);

    pub(in crate::link::egress) fn is_none(self) -> bool {
        self == Self::NONE
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::link::egress::metadata) struct DetachedIndex(u32);

/// Owns every node and its only mutable path.
/// Typed indices encode free, reserved, linked, and detached transitions.
pub(in crate::link::egress) struct MetadataPool<T> {
    nodes: Box<[RawCell<Node<T>>]>,
    free: Cell<u32>,
    credits: FairCreditPool<2>,
}

struct ReservedNode<'a, T> {
    pool: &'a MetadataPool<T>,
    index: ReservedIndex,
    armed: bool,
}

impl<T> ReservedNode<'_, T> {
    fn commit(mut self) {
        self.armed = false;
    }
}

impl<T> Drop for ReservedNode<'_, T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(node) = self.pool.take_reserved(self.index) {
            drop(node);
        } else {
            self.pool.release_reserved(self.index);
        }
    }
}

impl<T> MetadataPool<T> {
    pub(in crate::link::egress) fn with_config(config: Config) -> Self {
        let capacity = config.entries();
        Self {
            nodes: (0..capacity)
                .map(|index| {
                    RawCell::new(Node {
                        value: None,
                        next: if index + 1 == capacity {
                            NONE
                        } else {
                            (index + 1) as u32
                        },
                        bytes: 0,
                        resident: 0,
                    })
                })
                .collect(),
            free: Cell::new(if capacity == 0 { NONE } else { 0 }),
            credits: FairCreditPool::new([
                config.shared_entries as usize,
                config.shared_bytes as usize,
            ]),
        }
    }

    pub(in crate::link::egress::metadata) fn credit<'a>(
        &'a self,
        state: &'a FairCreditState<2>,
    ) -> FairCreditLane<'a, 2> {
        self.credits.bind(state)
    }

    fn update_node<R>(&self, index: u32, update: impl FnOnce(&mut Node<T>) -> R) -> R {
        // SAFETY: the pool owns every node and exposes mutations only through
        // the state-specific transitions in this impl. Their callbacks do not
        // reenter the pool, except `reserve_from`, which first removes its node
        // from every reachable list. RawCell's HRTB keeps shared references
        // scoped to `with_value` and `next`.
        unsafe { self.nodes[index as usize].with_mut(update) }
    }

    pub(in crate::link::egress) fn reserve_from<V, R>(
        &self,
        value: V,
        wrap: impl FnOnce(V) -> T,
        prepare: impl FnOnce(&mut T) -> (R, usize, usize),
    ) -> Result<(ReservedIndex, R), V> {
        let raw = self.free.get();
        if raw == NONE {
            return Err(value);
        }
        let index = ReservedIndex(raw);
        let mut reservation = ReservedNode {
            pool: self,
            index,
            armed: false,
        };
        let result = self.update_node(raw, |node| {
            reservation.armed = true;
            self.free.set(node.next);
            node.next = NONE;
            let (result, bytes, resident) = prepare(node.value.insert(wrap(value)));
            node.bytes = bytes;
            node.resident = resident;
            result
        });
        reservation.commit();
        Ok((index, result))
    }

    pub(in crate::link::egress) fn with_value<R>(
        &self,
        index: LinkedIndex,
        inspect: impl FnOnce(&T) -> R,
    ) -> Option<R> {
        self.nodes[index.0 as usize].with(|node| node.value.as_ref().map(inspect))
    }

    pub(in crate::link::egress) fn set_reserved_next(
        &self,
        index: ReservedIndex,
        next: ReservedIndex,
    ) {
        self.set_next(index.0, next.0);
    }

    pub(in crate::link::egress) fn set_linked_next(&self, index: LinkedIndex, next: LinkedIndex) {
        self.set_next(index.0, next.0);
    }

    fn set_next(&self, index: u32, next: u32) {
        self.update_node(index, |node| node.next = next);
    }

    pub(in crate::link::egress) fn next(&self, index: LinkedIndex) -> LinkedIndex {
        LinkedIndex(self.nodes[index.0 as usize].with(|node| node.next))
    }

    pub(in crate::link::egress) fn take_reserved(
        &self,
        index: ReservedIndex,
    ) -> Option<(ReservedIndex, T, usize, usize)> {
        self.take_node(index.0)
            .map(|(next, value, bytes, resident)| (ReservedIndex(next), value, bytes, resident))
    }

    fn take_node(&self, index: u32) -> Option<(u32, T, usize, usize)> {
        self.update_node(index, |node| {
            let value = node.value.take()?;
            let next = node.next;
            let bytes = node.bytes;
            let resident = node.resident;
            node.bytes = 0;
            node.resident = 0;
            node.next = self.free.replace(index);
            Some((next, value, bytes, resident))
        })
    }

    pub(in crate::link::egress::metadata) fn detach_value(
        &self,
        index: LinkedIndex,
    ) -> Option<(LinkedIndex, T, usize, usize, DetachedIndex)> {
        self.update_node(index.0, |node| {
            let value = node.value.take()?;
            let next = LinkedIndex(node.next);
            let bytes = node.bytes;
            let resident = node.resident;
            node.next = NONE;
            Some((next, value, bytes, resident, DetachedIndex(index.0)))
        })
    }

    pub(in crate::link::egress::metadata) fn restore_value(
        &self,
        index: DetachedIndex,
        value: T,
        next: LinkedIndex,
    ) -> LinkedIndex {
        self.update_node(index.0, |node| {
            debug_assert!(node.value.is_none());
            node.value = Some(value);
            node.next = next.0;
        });
        LinkedIndex(index.0)
    }

    pub(in crate::link::egress::metadata) fn release_detached(&self, index: DetachedIndex) {
        self.release_empty_node(index.0);
    }

    fn release_reserved(&self, index: ReservedIndex) {
        self.release_empty_node(index.0);
    }

    fn release_empty_node(&self, index: u32) {
        self.update_node(index, |node| {
            debug_assert!(node.value.is_none());
            node.bytes = 0;
            node.resident = 0;
            node.next = self.free.replace(index);
        });
    }

    pub(in crate::link::egress) fn drain_reserved(&self, head: &mut ReservedIndex) {
        self.drain_nodes(replace(head, ReservedIndex::NONE).0);
    }

    pub(in crate::link::egress::metadata) fn drain_linked(&self, head: &mut LinkedIndex) {
        self.drain_nodes(replace(head, LinkedIndex::NONE).0);
    }

    fn drain_nodes(&self, head: u32) {
        let mut drain = NodeDrain { pool: self, head };
        while let Some(value) = drain.take() {
            drop(value);
        }
    }

    pub(in crate::link::egress) fn front_consume(&self, index: LinkedIndex, bytes: usize) {
        self.update_node(index.0, |node| {
            debug_assert!(node.bytes >= bytes);
            node.bytes -= bytes;
        });
    }
}

struct NodeDrain<'a, T> {
    pool: &'a MetadataPool<T>,
    head: u32,
}

impl<T> NodeDrain<'_, T> {
    fn take(&mut self) -> Option<T> {
        if self.head == NONE {
            return None;
        }
        let (next, value, _, _) = self.pool.take_node(self.head)?;
        self.head = next;
        Some(value)
    }
}

impl<T> Drop for NodeDrain<'_, T> {
    fn drop(&mut self) {
        while let Some(value) = self.take() {
            drop(value);
        }
    }
}
