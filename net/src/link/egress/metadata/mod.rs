use std::cell::Cell;
use std::mem::size_of;

use o3::cell::RegionToken;
use o3::mem::FairCreditState;

pub(in crate::link::egress) mod pool;

use self::pool::{DetachedIndex, LinkedIndex, Pool, ReservedIndex};
use super::config::Config;

pub struct Arena<'d, T> {
    pub(super) pool: Pool<'d, T>,
    lanes: Box<[Lane]>,
}

impl<'d, T> Arena<'d, T> {
    pub fn with_config(token: &RegionToken<'d>, config: Config, lanes: usize) -> Self {
        assert!(lanes != 0, "egress metadata requires at least one lane");
        Self {
            pool: Pool::with_config(token, config),
            lanes: (0..lanes)
                .map(|lane| Lane::with_config(config, lanes, lane))
                .collect(),
        }
    }

    pub fn queue(&self, lane: usize) -> Queue<'_, 'd, T> {
        Queue::with_lane(&self.pool, &self.lanes[lane])
    }

    pub fn clear(&self, lane: usize, token: &mut RegionToken<'d>) {
        drop(self.queue(lane).detach_all(token));
    }
}

pub(in crate::link::egress) struct Lane {
    head: Cell<LinkedIndex>,
    tail: Cell<LinkedIndex>,
    len: Cell<usize>,
    bytes: Cell<usize>,
    resident: Cell<usize>,
    credit: FairCreditState<2>,
}

const _: () = assert!(size_of::<Lane>() == 64);

impl Lane {
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

pub struct Queue<'a, 'd, T> {
    pub(super) pool: &'a Pool<'d, T>,
    state: &'a Lane,
}

impl<T> Clone for Queue<'_, '_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Queue<'_, '_, T> {}

impl<'d, T> Queue<'_, 'd, T> {
    pub(in crate::link::egress) fn with_lane<'a>(
        pool: &'a Pool<'d, T>,
        state: &'a Lane,
    ) -> Queue<'a, 'd, T> {
        Queue { pool, state }
    }

    pub fn try_push_back(
        &self,
        token: &mut RegionToken<'d>,
        value: T,
        bytes: usize,
    ) -> Result<(), T> {
        let index = self.pool.reserve(token, value, bytes, bytes)?;
        if !self
            .pool
            .credit(&self.state.credit)
            .try_acquire_all([1, bytes])
        {
            let Some((_, value, _, _)) = self.pool.take_reserved(token, index) else {
                unreachable!()
            };
            return Err(value);
        }
        let index = index.into_linked();
        let tail = self.state.tail.replace(index);
        if tail.is_none() {
            self.state.head.set(index);
        } else {
            self.pool.set_linked_next(token, tail, index);
        }
        self.state.len.set(self.state.len.get() + 1);
        self.state.bytes.set(self.state.bytes.get() + bytes);
        self.state.resident.set(self.state.resident.get() + bytes);
        Ok(())
    }

    pub fn take_front<'token>(
        &self,
        token: &'token mut RegionToken<'d>,
    ) -> Option<(T, Front<'_, 'token, 'd, T>)> {
        let index = self.state.head.get();
        if index.is_none() {
            return None;
        }
        let (next, value, bytes, resident, index) = self.pool.detach_value(token, index)?;
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
            Front {
                pool: self.pool,
                state: self.state,
                token,
                index,
                bytes,
                resident,
                settled: false,
            },
        ))
    }

    pub(super) fn index_at(&self, token: &RegionToken<'d>, offset: usize) -> Option<LinkedIndex> {
        if offset >= self.state.len.get() {
            return None;
        }
        let mut index = self.state.head.get();
        for _ in 0..offset {
            index = self.pool.next(token, index);
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
        token: &mut RegionToken<'d>,
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
            self.pool.set_linked_next(token, old_tail, head);
        }
        self.state.len.set(self.state.len.get() + entries);
        self.state.bytes.set(self.state.bytes.get() + bytes);
        self.state
            .resident
            .set(self.state.resident.get() + resident);
        true
    }

    pub(super) fn consume_front_bytes(&self, token: &mut RegionToken<'d>, bytes: usize) {
        let head = self.state.head.get();
        debug_assert!(!head.is_none());
        self.pool.front_consume(token, head, bytes);
        self.state.bytes.set(self.state.bytes.get() - bytes);
    }

    pub fn detach_all<'token>(
        &self,
        token: &'token mut RegionToken<'d>,
    ) -> Detached<'_, 'token, 'd, T> {
        let index = self.state.head.replace(LinkedIndex::NONE);
        self.state.tail.set(LinkedIndex::NONE);
        let entries = self.state.len.take();
        self.state.bytes.take();
        let resident = self.state.resident.take();
        self.pool
            .credit(&self.state.credit)
            .release_all([entries, resident]);
        Detached {
            pool: self.pool,
            token,
            head: index,
        }
    }
}

pub struct Front<'a, 'token, 'd, T> {
    pool: &'a Pool<'d, T>,
    state: &'a Lane,
    token: &'token mut RegionToken<'d>,
    index: DetachedIndex,
    bytes: usize,
    resident: usize,
    settled: bool,
}

impl<'a, 'token, 'd, T> Front<'a, 'token, 'd, T> {
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Reborrows the region capability while this detached front remains the
    /// rollback owner. The callback may move the value into another branded
    /// collection; the caller must then release or restore this front.
    pub fn with_region<R>(&mut self, f: impl FnOnce(&mut RegionToken<'d>) -> R) -> R {
        f(self.token)
    }

    pub fn release(mut self) {
        self.pool.release_detached(self.token, self.index);
        self.pool
            .credit(&self.state.credit)
            .release_all([1, self.resident]);
        self.settled = true;
    }

    pub fn restore(mut self, value: T) {
        let head = self.state.head.get();
        let index = self.pool.restore_value(self.token, self.index, value, head);
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

impl<T> Drop for Front<'_, '_, '_, T> {
    fn drop(&mut self) {
        if !self.settled {
            self.pool.release_detached(self.token, self.index);
            self.pool
                .credit(&self.state.credit)
                .release_all([1, self.resident]);
            self.settled = true;
        }
    }
}

pub struct Detached<'a, 'token, 'd, T> {
    pool: &'a Pool<'d, T>,
    token: &'token mut RegionToken<'d>,
    head: LinkedIndex,
}

impl<T> Drop for Detached<'_, '_, '_, T> {
    fn drop(&mut self) {
        self.pool.drain_linked(self.token, &mut self.head);
    }
}
