use std::cell::Cell;

use o3::cell::RawCell;

use super::super::super::NONE;
use super::super::super::config::Config;
use super::super::super::credits::Credits;
use std::mem::replace;

struct Node<T> {
    value: Option<T>,
    next: u32,
    bytes: usize,
    resident: usize,
}

pub(crate) struct MetadataPool<T> {
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
    pub(crate) fn with_config(config: Config, lanes: usize) -> Self {
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

    pub(crate) fn reserve_from<V, R>(
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

    pub(crate) fn with_value<R>(&self, index: u32, inspect: impl FnOnce(&T) -> R) -> Option<R> {
        unsafe { self.nodes[index as usize].with(|node| node.value.as_ref().map(inspect)) }
    }

    pub(crate) fn set_sizes(&self, index: u32, bytes: usize, resident: usize) {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                node.bytes = bytes;
                node.resident = resident;
            })
        };
    }

    pub(crate) fn set_next(&self, index: u32, next: u32) {
        unsafe { self.nodes[index as usize].with_mut(|node| node.next = next) };
    }

    pub(crate) fn next(&self, index: u32) -> u32 {
        unsafe { self.nodes[index as usize].with(|node| node.next) }
    }

    pub(crate) fn take_node(&self, index: u32) -> Option<(u32, T, usize, usize)> {
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

    pub(crate) fn detach_value(&self, index: u32) -> Option<(u32, T, usize, usize)> {
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

    pub(crate) fn restore_value(&self, index: u32, value: T, next: u32) {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                debug_assert!(node.value.is_none());
                node.value = Some(value);
                node.next = next;
            })
        };
    }

    pub(crate) fn release_empty_node(&self, index: u32) {
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

    pub(crate) fn drain_nodes(&self, head: &mut u32) {
        let mut drain = NodeDrain {
            pool: self,
            head: replace(head, NONE),
        };
        while let Some(value) = drain.take() {
            drop(value);
        }
    }

    pub(crate) fn front_consume(&self, index: u32, bytes: usize) {
        unsafe {
            self.nodes[index as usize].with_mut(|node| {
                debug_assert!(node.bytes >= bytes);
                node.bytes -= bytes;
            })
        };
    }

    pub(crate) fn lanes(&self) -> usize {
        unsafe { self.credits.with(Credits::lanes) }
    }

    pub(crate) fn has_available(&self) -> bool {
        self.node_available.get() != 0
    }

    pub(crate) fn acquire(&self, lane: usize, entries: usize, bytes: usize) -> bool {
        unsafe {
            self.credits
                .with_mut(|credits| credits.acquire(lane, entries, bytes))
        }
    }

    pub(crate) fn release(&self, lane: usize, entries: usize, bytes: usize) {
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
