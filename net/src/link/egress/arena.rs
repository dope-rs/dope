use std::cell::Cell;

use o3::buffer::Shared;

use super::config::Config;
use super::metadata::raw::pool::MetadataPool;
use super::metadata::{MetadataArena, MetadataQueue};
use super::queue::Queue;
use super::raw::entry::{Entry, PreparedEntry};
use super::wire::WireArena;
use super::{EGRESS_CAP_BYTES, EGRESS_QUANTUM, NONE};

pub struct Arena<B = Shared> {
    entries: MetadataArena<Entry<B>>,
    wire: WireArena,
    next_lane: Cell<usize>,
    lanes: usize,
}

impl<B> Arena<B> {
    pub fn with_capacity(capacity: u32) -> Self {
        Self::with_limits(capacity, EGRESS_CAP_BYTES, 1)
    }

    pub fn with_limits(capacity: u32, bytes: u32, lanes: usize) -> Self {
        Self::with_config(Config::shared(capacity, bytes), lanes)
    }

    pub fn with_config(config: Config, lanes: usize) -> Self {
        assert!(lanes != 0, "egress arena requires at least one lane");
        let bytes = config.wire_bytes();
        Self {
            entries: MetadataArena::with_config(config, lanes),
            wire: WireArena::with_capacity(bytes),
            next_lane: Cell::new(0),
            lanes,
        }
    }
}

impl<B: AsRef<[u8]>> Arena<B> {
    pub fn queue<const IOV: usize>(&self) -> Queue<IOV, B> {
        let lane = self.next_lane.get() % self.lanes;
        self.next_lane.set(self.next_lane.get().wrapping_add(1));
        self.queue_for(lane)
    }

    pub fn queue_for<const IOV: usize>(&self, lane: usize) -> Queue<IOV, B> {
        assert!(lane < self.lanes, "egress queue lane out of bounds");
        Queue::with_arena(&self.entries, &self.wire, lane)
    }
}

impl<B> Default for Arena<B> {
    fn default() -> Self {
        Self::with_config(Config::default(), 1)
    }
}

pub(super) struct PreparedChain<'a, B> {
    pool: &'a MetadataPool<Entry<B>>,
    head: u32,
    tail: u32,
    entries: usize,
    bytes: usize,
    resident: usize,
}

impl<'a, B: AsRef<[u8]>> PreparedChain<'a, B> {
    pub(super) fn new(pool: &'a MetadataPool<Entry<B>>) -> Self {
        Self {
            pool,
            head: NONE,
            tail: NONE,
            entries: 0,
            bytes: 0,
            resident: 0,
        }
    }

    pub(super) fn push(&mut self, value: B) -> bool {
        match Entry::prepare_buffer(self.pool, value) {
            Ok(PreparedEntry::Empty) => true,
            Ok(PreparedEntry::Node {
                index,
                bytes,
                resident,
            }) => self.link(index, bytes, resident),
            Err(value) => {
                drop(value);
                false
            }
        }
    }

    pub(super) fn push_wire(&mut self, data: *const u8, len: usize) -> bool {
        match Entry::prepare_wire(self.pool, data, len) {
            Some(PreparedEntry::Empty) => true,
            Some(PreparedEntry::Node {
                index,
                bytes,
                resident,
            }) => self.link(index, bytes, resident),
            None => false,
        }
    }

    pub(super) fn push_copy(&mut self, src: &[u8]) -> bool {
        for chunk in src.chunks(EGRESS_QUANTUM) {
            match Entry::prepare_copy(self.pool, chunk) {
                Some(PreparedEntry::Empty) => {}
                Some(PreparedEntry::Node {
                    index,
                    bytes,
                    resident,
                }) => {
                    if !self.link(index, bytes, resident) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }

    pub(super) fn push_static(&mut self, src: &'static [u8]) -> bool {
        match Entry::prepare_static(self.pool, src) {
            Some(PreparedEntry::Empty) => true,
            Some(PreparedEntry::Node {
                index,
                bytes,
                resident,
            }) => self.link(index, bytes, resident),
            None => false,
        }
    }

    fn link(&mut self, index: u32, bytes: usize, resident: usize) -> bool {
        let Some(total_bytes) = self.bytes.checked_add(bytes) else {
            drop(self.pool.take_node(index));
            return false;
        };
        let Some(total_resident) = self.resident.checked_add(resident) else {
            drop(self.pool.take_node(index));
            return false;
        };
        if self.tail == NONE {
            self.head = index;
        } else {
            self.pool.set_next(self.tail, index);
        }
        self.tail = index;
        self.entries += 1;
        self.bytes = total_bytes;
        self.resident = total_resident;
        true
    }

    pub(super) fn commit(mut self, queue: &MetadataQueue<Entry<B>>) -> bool {
        if !queue.commit_prepared(
            self.head,
            self.tail,
            self.entries,
            self.bytes,
            self.resident,
        ) {
            return false;
        }
        self.head = NONE;
        true
    }
}

impl<B> Drop for PreparedChain<'_, B> {
    fn drop(&mut self) {
        self.pool.drain_nodes(&mut self.head);
    }
}
