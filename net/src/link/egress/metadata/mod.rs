pub(super) mod raw;

use std::cell::Cell;
use std::mem::size_of;

use self::raw::pool::{DetachedIndex, LinkedIndex, MetadataPool, ReservedIndex};
use super::config::Config;
use o3::mem::FairCreditState;

pub struct MetadataArena<T> {
    pub(super) pool: MetadataPool<T>,
    lanes: Box<[MetadataLane]>,
}

impl<T> MetadataArena<T> {
    pub fn with_config(config: Config, lanes: usize) -> Self {
        assert!(lanes != 0, "egress metadata requires at least one lane");
        Self {
            pool: MetadataPool::with_config(config),
            lanes: (0..lanes)
                .map(|lane| MetadataLane::with_config(config, lanes, lane))
                .collect(),
        }
    }

    pub fn queue(&self, lane: usize) -> MetadataQueue<'_, T> {
        MetadataQueue::with_lane(&self.pool, &self.lanes[lane])
    }

    pub fn clear(&self, lane: usize) {
        drop(self.queue(lane).detach_all());
    }
}

pub(in crate::link::egress) struct MetadataLane {
    head: Cell<LinkedIndex>,
    tail: Cell<LinkedIndex>,
    len: Cell<usize>,
    bytes: Cell<usize>,
    resident: Cell<usize>,
    credit: FairCreditState<2>,
}

const _: () = assert!(size_of::<MetadataLane>() == 64);

impl MetadataLane {
    pub(in crate::link::egress) fn with_config(config: Config, lanes: usize, lane: usize) -> Self {
        Self {
            head: Cell::new(LinkedIndex::NONE),
            tail: Cell::new(LinkedIndex::NONE),
            len: Cell::new(0),
            bytes: Cell::new(0),
            resident: Cell::new(0),
            credit: FairCreditState::split_at(
                [
                    config.reserved_entries as usize,
                    config.reserved_bytes as usize,
                ],
                lanes,
                lane,
            ),
        }
    }
}

pub struct MetadataQueue<'a, T> {
    pub(super) pool: &'a MetadataPool<T>,
    state: &'a MetadataLane,
}

impl<T> Clone for MetadataQueue<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for MetadataQueue<'_, T> {}

impl<T> MetadataQueue<'_, T> {
    pub(in crate::link::egress) fn with_lane<'a>(
        pool: &'a MetadataPool<T>,
        state: &'a MetadataLane,
    ) -> MetadataQueue<'a, T> {
        MetadataQueue { pool, state }
    }

    pub fn try_push_back(&self, value: T, bytes: usize) -> Result<(), T> {
        let index = match self
            .pool
            .reserve_from(value, |value| value, |_| ((), bytes, bytes))
        {
            Ok((index, _)) => index,
            Err(value) => return Err(value),
        };
        if !self
            .pool
            .credit(&self.state.credit)
            .try_acquire_all([1, bytes])
        {
            let Some((_, value, _, _)) = self.pool.take_reserved(index) else {
                unreachable!()
            };
            return Err(value);
        }
        let index = index.into_linked();
        let tail = self.state.tail.replace(index);
        if tail.is_none() {
            self.state.head.set(index);
        } else {
            self.pool.set_linked_next(tail, index);
        }
        self.state.len.set(self.state.len.get() + 1);
        self.state.bytes.set(self.state.bytes.get() + bytes);
        self.state.resident.set(self.state.resident.get() + bytes);
        Ok(())
    }

    pub fn take_front(&self) -> Option<(T, MetadataFront<'_, T>)> {
        let index = self.state.head.get();
        if index.is_none() {
            return None;
        }
        let (next, value, bytes, resident, index) = self.pool.detach_value(index)?;
        self.state.head.set(next);
        if next.is_none() {
            self.state.tail.set(LinkedIndex::NONE);
        }
        self.state.len.set(self.state.len.get() - 1);
        self.state.bytes.set(self.state.bytes.get() - bytes);
        self.state
            .resident
            .set(self.state.resident.get() - resident);
        Some((
            value,
            MetadataFront {
                pool: self.pool,
                state: self.state,
                index,
                bytes,
                resident,
                settled: false,
            },
        ))
    }

    pub(super) fn index_at(&self, offset: usize) -> Option<LinkedIndex> {
        if offset >= self.state.len.get() {
            return None;
        }
        let mut index = self.state.head.get();
        for _ in 0..offset {
            index = self.pool.next(index);
        }
        Some(index)
    }

    pub fn len(&self) -> usize {
        self.state.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.state.len.get() == 0
    }

    pub fn bytes(&self) -> usize {
        self.state.bytes.get()
    }

    pub(super) fn head(&self) -> LinkedIndex {
        self.state.head.get()
    }

    pub(super) fn commit_prepared(
        &self,
        head: ReservedIndex,
        tail: ReservedIndex,
        entries: usize,
        bytes: usize,
        resident: usize,
    ) -> bool {
        if entries == 0 {
            return true;
        }
        if !self
            .pool
            .credit(&self.state.credit)
            .try_acquire_all([entries, resident])
        {
            return false;
        }
        let head = head.into_linked();
        let tail = tail.into_linked();
        let old_tail = self.state.tail.replace(tail);
        if old_tail.is_none() {
            self.state.head.set(head);
        } else {
            self.pool.set_linked_next(old_tail, head);
        }
        self.state.len.set(self.state.len.get() + entries);
        self.state.bytes.set(self.state.bytes.get() + bytes);
        self.state
            .resident
            .set(self.state.resident.get() + resident);
        true
    }

    pub(super) fn consume_front_bytes(&self, bytes: usize) {
        let head = self.state.head.get();
        debug_assert!(!head.is_none());
        self.pool.front_consume(head, bytes);
        self.state.bytes.set(self.state.bytes.get() - bytes);
    }

    pub fn detach_all(&self) -> DetachedValues<'_, T> {
        let index = self.state.head.replace(LinkedIndex::NONE);
        self.state.tail.set(LinkedIndex::NONE);
        let entries = self.state.len.take();
        self.state.bytes.take();
        let resident = self.state.resident.take();
        self.pool
            .credit(&self.state.credit)
            .release_all([entries, resident]);
        DetachedValues {
            pool: self.pool,
            head: index,
        }
    }
}

pub struct MetadataFront<'a, T> {
    pool: &'a MetadataPool<T>,
    state: &'a MetadataLane,
    index: DetachedIndex,
    bytes: usize,
    resident: usize,
    settled: bool,
}

impl<T> MetadataFront<'_, T> {
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn release(mut self) {
        self.pool.release_detached(self.index);
        self.pool
            .credit(&self.state.credit)
            .release_all([1, self.resident]);
        self.settled = true;
    }

    pub fn restore(mut self, value: T) {
        let head = self.state.head.get();
        let index = self.pool.restore_value(self.index, value, head);
        self.state.head.set(index);
        if head.is_none() {
            self.state.tail.set(index);
        }
        self.state.len.set(self.state.len.get() + 1);
        self.state.bytes.set(self.state.bytes.get() + self.bytes);
        self.state
            .resident
            .set(self.state.resident.get() + self.resident);
        self.settled = true;
    }
}

impl<T> Drop for MetadataFront<'_, T> {
    fn drop(&mut self) {
        if !self.settled {
            self.pool.release_detached(self.index);
            self.pool
                .credit(&self.state.credit)
                .release_all([1, self.resident]);
            self.settled = true;
        }
    }
}

pub struct DetachedValues<'a, T> {
    pool: &'a MetadataPool<T>,
    head: LinkedIndex,
}

impl<T> Drop for DetachedValues<'_, T> {
    fn drop(&mut self) {
        self.pool.drain_linked(&mut self.head);
    }
}

impl<T> Drop for MetadataArena<T> {
    fn drop(&mut self) {
        for lane in 0..self.lanes.len() {
            self.clear(lane);
        }
    }
}
