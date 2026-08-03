use std::mem::size_of;
use std::time::Instant;

use o3::cell::{RegionCell, RegionToken};
use o3::collections::IndexedMinHeap;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key {
    pub(super) deadline: Instant,
    pub(super) epoch: u32,
}

pub(super) struct Heap<'d> {
    heap: RegionCell<'d, IndexedMinHeap<Key>>,
}

const _: () = assert!(
    size_of::<RegionCell<'static, IndexedMinHeap<Key>>>() == size_of::<IndexedMinHeap<Key>>()
);

impl<'d> Heap<'d> {
    pub(super) fn new(token: &RegionToken<'d>, capacity: usize) -> Self {
        let _ = token;
        Self {
            heap: RegionCell::new(IndexedMinHeap::with_capacity(capacity)),
        }
    }

    pub(super) fn remove(&self, token: &mut RegionToken<'d>, slot: usize) {
        self.heap.borrow_mut(token).remove(slot);
    }

    pub(super) fn insert(&self, token: &mut RegionToken<'d>, slot: usize, key: Key) {
        let Some(entry) = self.heap.borrow_mut(token).vacant_entry(slot) else {
            unreachable!()
        };
        entry.insert(key);
    }

    pub(super) fn peek(&self, token: &RegionToken<'d>) -> Option<Key> {
        self.heap.borrow(token).peek().map(|(_, key)| *key)
    }

    pub(super) fn pop(&self, token: &mut RegionToken<'d>) -> Option<(usize, Key)> {
        self.heap.borrow_mut(token).pop()
    }
}
