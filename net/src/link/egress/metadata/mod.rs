use std::{mem, process};

use o3::cell::region;

use crate::link::egress::{data, metadata::pool::indices};

pub mod arena;
mod free;
pub(in crate::link::egress) mod pool;
mod state;

pub(in crate::link) struct Lane<'d> {
    cells: state::State<'d>,
    credit: pool::CreditState,
}

const _: () = assert!(mem::size_of::<Lane<'static>>() == 64);

impl Lane<'_> {
    pub(in crate::link::egress) fn new(credit: pool::CreditState) -> Self {
        Self {
            cells: state::State::new(),
            credit,
        }
    }

    pub(in crate::link::egress) fn is_empty(&self) -> bool {
        self.cells.len.get() == 0
    }

    pub(in crate::link) fn bytes(&self) -> usize {
        self.cells.bytes.get()
    }
}

pub struct Queue<'a, 'd, T> {
    pub(super) pool: &'a pool::Pool<'d, T>,
    state: &'a Lane<'d>,
    credit: pool::CreditLane<'a>,
}

/// A located queue head which remains linked until consumed.
#[must_use]
pub struct FrontEntry<'queue, 'token, 'd, T> {
    queue: Queue<'queue, 'd, T>,
    token: &'token mut region::Token<'d>,
    index: indices::LinkedIndex<'d>,
}

impl<T> Clone for Queue<'_, '_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Queue<'_, '_, T> {}

impl<'queue, 'd, T> Queue<'queue, 'd, T> {
    pub fn front<'token>(
        self,
        token: &'token mut region::Token<'d>,
    ) -> Option<FrontEntry<'queue, 'token, 'd, T>> {
        let index = self.state.cells.head.get();
        (!index.is_none()).then_some(FrontEntry {
            queue: self,
            token,
            index,
        })
    }

    pub(in crate::link::egress) fn with_lane<'a>(
        pool: &'a pool::Pool<'d, T>,
        state: &'a Lane<'d>,
    ) -> Queue<'a, 'd, T> {
        Queue {
            pool,
            state,
            credit: pool.credit(&state.credit),
        }
    }

    pub(super) fn try_push_back_charged(
        &self,
        token: &mut region::Token<'d>,
        value: T,
        bytes: usize,
        resident: usize,
    ) -> Result<(), T> {
        let reservation = self.pool.reserve(token, value, bytes, resident)?;
        if !self.try_acquire(1, resident) {
            return Err(reservation.rollback(self.pool, token));
        }
        let index = reservation.install(self.pool, token, |value| value);
        self.commit_acquired(token, index, index, 1, bytes, resident);
        Ok(())
    }

    pub fn take_front<'token>(
        self,
        token: &'token mut region::Token<'d>,
    ) -> Option<(T, Front<'queue, 'token, 'd, T>)> {
        Some(self.front(token)?.take())
    }

    pub fn len(&self) -> usize {
        self.state.cells.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.state.cells.len.get() == 0
    }

    pub fn bytes(&self) -> usize {
        self.state.cells.bytes.get()
    }

    /// Starts an off-queue retained chain. Until [`Prepared::commit`], no
    /// value is visible and dropping the chain releases every reserved node.
    pub fn prepare<'token>(
        self,
        token: &'token mut region::Token<'d>,
    ) -> Prepared<'queue, 'token, 'd, T> {
        Prepared::new(self, token)
    }

    pub(super) fn head(&self) -> indices::LinkedIndex<'d> {
        self.state.cells.head.get()
    }

    pub(super) fn commit_prepared(
        &self,
        token: &mut region::Token<'d>,
        head: indices::ReservedIndex<'d>,
        tail: indices::ReservedIndex<'d>,
        entries: usize,
        bytes: usize,
        resident: usize,
    ) -> bool {
        if entries == 0 {
            return true;
        }
        if !self.try_acquire(entries, resident) {
            return false;
        }
        self.commit_acquired(token, head, tail, entries, bytes, resident);
        true
    }

    pub(super) fn try_acquire(&self, entries: usize, resident: usize) -> bool {
        self.credit.try_acquire([entries, resident])
    }

    pub(super) fn commit_acquired(
        &self,
        token: &mut region::Token<'d>,
        head: indices::ReservedIndex<'d>,
        tail: indices::ReservedIndex<'d>,
        entries: usize,
        bytes: usize,
        resident: usize,
    ) {
        let head = head.into_linked();
        let tail = tail.into_linked();
        let old_tail = self.state.cells.tail.replace(tail);
        if old_tail.is_none() {
            self.state.cells.head.set(head);
        } else {
            old_tail.set_next(self.pool, token, head);
        }
        self.state
            .cells
            .len
            .set(self.state.cells.len.get() + entries);
        self.state
            .cells
            .bytes
            .set(self.state.cells.bytes.get() + bytes);
        self.state
            .cells
            .resident
            .set(self.state.cells.resident.get() + resident);
    }

    pub(super) fn consume_front_bytes(&self, token: &mut region::Token<'d>, bytes: usize) {
        let head = self.state.cells.head.get();
        debug_assert!(!head.is_none());
        head.consume(self.pool, token, bytes);
        self.state
            .cells
            .bytes
            .set(self.state.cells.bytes.get() - bytes);
    }

    pub(in crate::link::egress) fn clear_one(&self, token: &mut region::Token<'d>) {
        let Some((value, front)) = self.take_front(token) else {
            return;
        };
        front.release();
        drop(value);
    }
}

impl<'queue, 'token, 'd, T> FrontEntry<'queue, 'token, 'd, T> {
    pub fn take(self) -> (T, Front<'queue, 'token, 'd, T>) {
        let Self {
            queue,
            token,
            index,
        } = self;
        let Some((next, value, bytes, resident, index)) = index.detach(queue.pool, token) else {
            process::abort();
        };
        queue.state.cells.head.set(next);
        if next.is_none() {
            queue.state.cells.tail.set(indices::LinkedIndex::NONE);
        }
        queue.state.cells.len.set(queue.state.cells.len.get() - 1);
        queue
            .state
            .cells
            .bytes
            .set(queue.state.cells.bytes.get() - bytes);
        queue
            .state
            .cells
            .resident
            .set(queue.state.cells.resident.get() - resident);
        (
            value,
            Front {
                pool: queue.pool,
                state: queue.state,
                credit: queue.credit,
                token,
                index,
                bytes,
                resident,
                settled: false,
            },
        )
    }
}

/// A linear, rollback-owned chain of retained queue entries.
#[must_use]
pub struct Prepared<'queue, 'token, 'd, T> {
    queue: Queue<'queue, 'd, T>,
    token: &'token mut region::Token<'d>,
    head: indices::ReservedIndex<'d>,
    tail: indices::ReservedIndex<'d>,
    entries: usize,
    bytes: usize,
    resident: usize,
    failed: bool,
}

impl<'queue, 'token, 'd, T> Prepared<'queue, 'token, 'd, T> {
    fn new(queue: Queue<'queue, 'd, T>, token: &'token mut region::Token<'d>) -> Self {
        Self {
            queue,
            token,
            head: indices::ReservedIndex::NONE,
            tail: indices::ReservedIndex::NONE,
            entries: 0,
            bytes: 0,
            resident: 0,
            failed: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    pub(in crate::link::egress) fn try_push_mapped<U>(
        &mut self,
        value: U,
        bytes: usize,
        resident: usize,
        map: impl FnOnce(U) -> T,
    ) -> Result<(), U> {
        if self.failed {
            return Err(value);
        }
        let Some(entries) = self.entries.checked_add(1) else {
            self.failed = true;
            return Err(value);
        };
        let Some(total_bytes) = self.bytes.checked_add(bytes) else {
            self.failed = true;
            return Err(value);
        };
        let Some(total_resident) = self.resident.checked_add(resident) else {
            self.failed = true;
            return Err(value);
        };
        let index = match self
            .queue
            .pool
            .reserve_mapped(self.token, value, bytes, resident, map)
        {
            Ok(index) => index,
            Err(value) => {
                self.failed = true;
                return Err(value);
            }
        };
        if self.tail.is_none() {
            self.head = index;
        } else {
            self.tail.set_next(self.queue.pool, self.token, index);
        }
        self.tail = index;
        self.entries = entries;
        self.bytes = total_bytes;
        self.resident = total_resident;
        Ok(())
    }

    /// Atomically publishes the complete chain after acquiring all lane
    /// credits. A poisoned or over-capacity chain publishes nothing.
    pub fn commit(mut self) -> bool {
        if self.failed
            || !self.queue.commit_prepared(
                self.token,
                self.head,
                self.tail,
                self.entries,
                self.bytes,
                self.resident,
            )
        {
            return false;
        }
        self.head = indices::ReservedIndex::NONE;
        true
    }
}

impl<'d, T: data::Payload<'d>> Prepared<'_, '_, 'd, T> {
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        let bytes = value.as_ref().len();
        if bytes == 0 {
            drop(value);
            return Ok(());
        }
        let resident = value.resident_bytes();
        self.try_push_mapped(value, bytes, resident, |value| value)
    }
}

impl<T> Drop for Prepared<'_, '_, '_, T> {
    fn drop(&mut self) {
        self.head.drain(self.queue.pool, self.token);
    }
}

impl<'d, T: data::Payload<'d>> Queue<'_, 'd, T> {
    pub fn try_push_back(&self, token: &mut region::Token<'d>, value: T) -> Result<(), T> {
        let bytes = value.as_ref().len();
        if bytes == 0 {
            drop(value);
            return Ok(());
        }
        let resident = value.resident_bytes();
        self.try_push_back_charged(token, value, bytes, resident)
    }
}

pub struct Front<'a, 'token, 'd, T> {
    pool: &'a pool::Pool<'d, T>,
    state: &'a Lane<'d>,
    credit: pool::CreditLane<'a>,
    token: &'token mut region::Token<'d>,
    index: indices::DetachedIndex<'d>,
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
    pub fn with_region<R>(&mut self, f: impl FnOnce(&mut region::Token<'d>) -> R) -> R {
        f(self.token)
    }

    pub fn release(mut self) {
        self.index.release(self.pool, self.token);
        self.credit.release([1, self.resident]);
        self.settled = true;
    }

    pub(in crate::link::egress) fn restore_unchanged(mut self, value: T) {
        let head = self.state.cells.head.get();
        let index = self.index.restore(
            self.pool,
            self.token,
            value,
            head,
            self.bytes,
            self.resident,
        );
        self.state.cells.head.set(index);
        if head.is_none() {
            self.state.cells.tail.set(index);
        }
        self.state.cells.len.set(self.state.cells.len.get() + 1);
        self.state
            .cells
            .bytes
            .set(self.state.cells.bytes.get() + self.bytes);
        self.state
            .cells
            .resident
            .set(self.state.cells.resident.get() + self.resident);
        self.settled = true;
    }
}

impl<'a, 'token, 'd, T: data::Payload<'d>> Front<'a, 'token, 'd, T> {
    /// Restores a returned value after validating its own logical and retained
    /// storage charges. Failure releases the detached queue entry and returns
    /// the value to the caller.
    pub fn try_restore(mut self, value: T) -> Result<(), T> {
        let bytes = value.as_ref().len();
        if bytes == 0 {
            self.index.release(self.pool, self.token);
            self.credit.release([1, self.resident]);
            self.settled = true;
            drop(value);
            return Ok(());
        }
        let resident = value.resident_bytes();
        let current_bytes = self.state.cells.bytes.get();
        let current_resident = self.state.cells.resident.get();
        if current_bytes.checked_add(bytes).is_none()
            || current_resident.checked_add(resident).is_none()
        {
            return Err(value);
        }
        if resident > self.resident && !self.credit.try_acquire([0, resident - self.resident]) {
            return Err(value);
        }
        let head = self.state.cells.head.get();
        let index = self
            .index
            .restore(self.pool, self.token, value, head, bytes, resident);
        self.state.cells.head.set(index);
        if head.is_none() {
            self.state.cells.tail.set(index);
        }
        self.state.cells.len.set(self.state.cells.len.get() + 1);
        self.state.cells.bytes.set(current_bytes + bytes);
        self.state.cells.resident.set(current_resident + resident);
        if self.resident > resident {
            self.credit.release([0, self.resident - resident]);
        }
        self.settled = true;
        Ok(())
    }
}

impl<T> Drop for Front<'_, '_, '_, T> {
    fn drop(&mut self) {
        if !self.settled {
            self.index.release(self.pool, self.token);
            self.credit.release([1, self.resident]);
            self.settled = true;
        }
    }
}
