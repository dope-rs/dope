use std::cell::Cell;
use std::mem;
use std::ptr::NonNull;
use std::rc::Rc;

use o3::cell::RawCell;

use super::NONE;
use super::config::Config;
use super::credits::Credits;

struct Node<T> {
    value: Option<T>,
    next: u32,
    bytes: usize,
    resident: usize,
}

pub(super) struct MetadataPool<T> {
    nodes: Box<[RawCell<Node<T>>]>,
    free: Cell<u32>,
    node_available: Cell<usize>,
    credits: RawCell<Credits>,
}

struct ReservedNode<'a, T> {
    pool: &'a MetadataPool<T>,
    index: u32,
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
        if let Some(node) = self.pool.take_node(self.index) {
            drop(node);
        } else {
            self.pool.release_empty_node(self.index);
        }
    }
}

impl<T> MetadataPool<T> {
    fn with_config(config: Config, lanes: usize) -> Self {
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
            node_available: Cell::new(capacity),
            credits: RawCell::new(Credits::with_config(config, lanes)),
        }
    }

    pub(super) fn reserve_from<V, R>(
        &self,
        value: V,
        wrap: impl FnOnce(V) -> T,
        inspect: impl FnOnce(&mut T) -> R,
    ) -> Result<(u32, R), V> {
        let index = self.free.get();
        if index == NONE {
            return Err(value);
        }
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                self.free.set(node.next);
                self.node_available.set(self.node_available.get() - 1);
                node.next = NONE;
                node.bytes = 0;
                node.resident = 0;
                let reservation = ReservedNode {
                    pool: self,
                    index,
                    armed: true,
                };
                let result = inspect(node.value.insert(wrap(value)));
                reservation.commit();
                Ok((index, result))
            })
        }
    }

    pub(super) fn value_ptr(&self, index: u32) -> Option<NonNull<T>> {
        unsafe { self.nodes[index as usize].with(|node| node.value.as_ref().map(NonNull::from)) }
    }

    pub(super) fn set_sizes(&self, index: u32, bytes: usize, resident: usize) {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                node.bytes = bytes;
                node.resident = resident;
            })
        };
    }

    pub(super) fn set_next(&self, index: u32, next: u32) {
        unsafe { self.nodes[index as usize].with_mut(|node| node.next = next) };
    }

    pub(super) fn next(&self, index: u32) -> u32 {
        unsafe { self.nodes[index as usize].with(|node| node.next) }
    }

    pub(super) fn take_node(&self, index: u32) -> Option<(u32, T, usize, usize)> {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                let value = node.value.take()?;
                let next = node.next;
                let bytes = node.bytes;
                let resident = node.resident;
                node.bytes = 0;
                node.resident = 0;
                node.next = self.free.replace(index);
                self.node_available.set(self.node_available.get() + 1);
                Some((next, value, bytes, resident))
            })
        }
    }

    fn detach_value(&self, index: u32) -> Option<(u32, T, usize, usize)> {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                let value = node.value.take()?;
                let next = node.next;
                let bytes = node.bytes;
                let resident = node.resident;
                node.next = NONE;
                Some((next, value, bytes, resident))
            })
        }
    }

    fn restore_value(&self, index: u32, value: T, next: u32) {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                debug_assert!(node.value.is_none());
                node.value = Some(value);
                node.next = next;
            })
        };
    }

    fn release_empty_node(&self, index: u32) {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                debug_assert!(node.value.is_none());
                node.bytes = 0;
                node.resident = 0;
                node.next = self.free.replace(index);
                self.node_available.set(self.node_available.get() + 1);
            })
        };
    }

    pub(super) fn drain_nodes(&self, head: &mut u32) {
        let mut drain = NodeDrain {
            pool: self,
            head: mem::replace(head, NONE),
        };
        while let Some(value) = drain.take() {
            drop(value);
        }
    }

    fn front_consume(&self, index: u32, bytes: usize) {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                debug_assert!(node.bytes >= bytes);
                node.bytes -= bytes;
            })
        };
    }

    fn acquire(&self, lane: usize, entries: usize, bytes: usize) -> bool {
        unsafe {
            self.credits
                .with_mut(|credits| credits.acquire(lane, entries, bytes))
        }
    }

    fn release(&self, lane: usize, entries: usize, bytes: usize) {
        unsafe {
            self.credits
                .with_mut(|credits| credits.release(lane, entries, bytes))
        };
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

pub struct MetadataArena<T> {
    pub(super) pool: Rc<MetadataPool<T>>,
}

impl<T> Clone for MetadataArena<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

impl<T> MetadataArena<T> {
    pub fn with_config(config: Config, lanes: usize) -> Self {
        Self {
            pool: Rc::new(MetadataPool::with_config(config, lanes)),
        }
    }
}

pub struct MetadataQueue<T> {
    pub(super) arena: MetadataArena<T>,
    pub(super) head: Cell<u32>,
    tail: Cell<u32>,
    len: Cell<usize>,
    bytes: Cell<usize>,
    resident: Cell<usize>,
    lane: usize,
    accepting: Cell<bool>,
}

impl<T> MetadataQueue<T> {
    pub fn new(arena: &MetadataArena<T>, lane: usize) -> Self {
        assert!(lane < unsafe { arena.pool.credits.with(Credits::lanes) });
        Self {
            arena: arena.clone(),
            head: Cell::new(NONE),
            tail: Cell::new(NONE),
            len: Cell::new(0),
            bytes: Cell::new(0),
            resident: Cell::new(0),
            lane,
            accepting: Cell::new(true),
        }
    }

    pub fn try_push_back(&self, value: T, bytes: usize) -> Result<(), T> {
        if !self.accepting.get()
            || self.arena.pool.node_available.get() == 0
            || !self.arena.pool.acquire(self.lane, 1, bytes)
        {
            return Err(value);
        }
        let index = match self.arena.pool.reserve_from(value, |value| value, |_| ()) {
            Ok((index, _)) => index,
            Err(value) => {
                self.arena.pool.release(self.lane, 1, bytes);
                return Err(value);
            }
        };
        self.arena.pool.set_sizes(index, bytes, bytes);
        let tail = self.tail.replace(index);
        if tail == NONE {
            self.head.set(index);
        } else {
            self.arena.pool.set_next(tail, index);
        }
        self.len.set(self.len.get() + 1);
        self.bytes.set(self.bytes.get() + bytes);
        self.resident.set(self.resident.get() + bytes);
        Ok(())
    }

    pub fn take_front(&self) -> Option<(T, MetadataFront<'_, T>)> {
        let index = self.head.get();
        if index == NONE {
            return None;
        }
        let (next, value, bytes, resident) = self.arena.pool.detach_value(index)?;
        self.head.set(next);
        if next == NONE {
            self.tail.set(NONE);
        }
        self.len.set(self.len.get() - 1);
        self.bytes.set(self.bytes.get() - bytes);
        self.resident.set(self.resident.get() - resident);
        Some((
            value,
            MetadataFront {
                queue: self,
                index,
                bytes,
                resident,
                settled: false,
            },
        ))
    }

    pub(super) fn index_at(&self, offset: usize) -> Option<u32> {
        if offset >= self.len.get() {
            return None;
        }
        let mut index = self.head.get();
        for _ in 0..offset {
            index = self.arena.pool.next(index);
        }
        Some(index)
    }

    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    pub fn bytes(&self) -> usize {
        self.bytes.get()
    }

    pub(super) fn commit_prepared(
        &self,
        head: u32,
        tail: u32,
        entries: usize,
        bytes: usize,
        resident: usize,
    ) -> bool {
        if entries == 0 {
            return true;
        }
        if !self.accepting.get() || !self.arena.pool.acquire(self.lane, entries, resident) {
            return false;
        }
        let old_tail = self.tail.replace(tail);
        if old_tail == NONE {
            self.head.set(head);
        } else {
            self.arena.pool.set_next(old_tail, head);
        }
        self.len.set(self.len.get() + entries);
        self.bytes.set(self.bytes.get() + bytes);
        self.resident.set(self.resident.get() + resident);
        true
    }

    pub(super) fn consume_front_bytes(&self, bytes: usize) {
        let head = self.head.get();
        debug_assert!(head != NONE);
        self.arena.pool.front_consume(head, bytes);
        self.bytes.set(self.bytes.get() - bytes);
    }

    pub fn detach_all(&self) -> DetachedValues<'_, T> {
        self.detach_values(true)
    }

    fn detach_values(&self, reopen: bool) -> DetachedValues<'_, T> {
        self.accepting.set(false);
        let index = self.head.replace(NONE);
        self.tail.set(NONE);
        let entries = self.len.take();
        self.bytes.take();
        let resident = self.resident.take();
        self.arena.pool.release(self.lane, entries, resident);
        if reopen {
            self.accepting.set(true);
        }
        DetachedValues {
            pool: &self.arena.pool,
            head: index,
        }
    }
}

pub struct MetadataFront<'a, T> {
    queue: &'a MetadataQueue<T>,
    index: u32,
    bytes: usize,
    resident: usize,
    settled: bool,
}

impl<T> MetadataFront<'_, T> {
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn release(mut self) {
        self.queue.arena.pool.release_empty_node(self.index);
        self.queue
            .arena
            .pool
            .release(self.queue.lane, 1, self.resident);
        self.settled = true;
    }

    pub fn restore(mut self, value: T) {
        let head = self.queue.head.replace(self.index);
        self.queue.arena.pool.restore_value(self.index, value, head);
        if head == NONE {
            self.queue.tail.set(self.index);
        }
        self.queue.len.set(self.queue.len.get() + 1);
        self.queue.bytes.set(self.queue.bytes.get() + self.bytes);
        self.queue
            .resident
            .set(self.queue.resident.get() + self.resident);
        self.settled = true;
    }
}

impl<T> Drop for MetadataFront<'_, T> {
    fn drop(&mut self) {
        if !self.settled {
            self.queue.arena.pool.release_empty_node(self.index);
            self.queue
                .arena
                .pool
                .release(self.queue.lane, 1, self.resident);
            self.settled = true;
        }
    }
}

pub struct DetachedValues<'a, T> {
    pool: &'a MetadataPool<T>,
    head: u32,
}

impl<T> Drop for DetachedValues<'_, T> {
    fn drop(&mut self) {
        self.pool.drain_nodes(&mut self.head);
    }
}

impl<T> Drop for MetadataQueue<T> {
    fn drop(&mut self) {
        drop(self.detach_values(false));
    }
}
