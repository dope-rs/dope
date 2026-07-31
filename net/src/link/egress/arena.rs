use o3::buffer::Shared;

use super::EGRESS_QUANTUM;
use super::config::Config;
use super::metadata::MetadataQueue;
use super::metadata::raw::pool::{MetadataPool, ReservedIndex};
use super::queue::{Queue, QueueState};
use super::raw::entry::{Entry, PreparedEntry};
use super::storage::Storage;
use super::wire::WireArena;

pub struct Arena<'pool, B = Shared, const IOV: usize = 32> {
    lanes: Box<[QueueState<'pool, IOV>]>,
    entries: MetadataPool<Entry<B>>,
    wire: WireArena<'pool>,
    next_lane: usize,
}

impl<'pool, B, const IOV: usize> Arena<'pool, B, IOV> {
    pub fn with_config(storage: &'pool Storage, config: Config, lanes: usize) -> Self {
        assert!(lanes != 0, "egress arena requires at least one lane");
        assert_eq!(
            storage.config.wire_bytes(),
            config.wire_bytes(),
            "egress storage and arena wire capacities must match"
        );
        Self {
            lanes: (0..lanes)
                .map(|lane| QueueState::with_config(config, lanes, lane))
                .collect(),
            entries: MetadataPool::with_config(config),
            wire: WireArena::new(&storage.wire),
            next_lane: 0,
        }
    }

    pub fn clear(&mut self, lane: usize) {
        self.lanes[lane].clear(&self.entries);
    }
}

impl<'pool, B: AsRef<[u8]>, const IOV: usize> Arena<'pool, B, IOV> {
    pub fn try_enqueue(&self, lane: usize, bytes: B) -> Result<(), B> {
        Queue::<IOV, B>::enqueue(
            MetadataQueue::with_lane(&self.entries, &self.lanes[lane].metadata),
            bytes,
        )
    }

    pub fn bytes(&self, lane: usize) -> usize {
        MetadataQueue::with_lane(&self.entries, &self.lanes[lane].metadata).bytes()
    }

    pub fn queue(&mut self) -> Queue<'_, 'pool, IOV, B> {
        let lane = self.next_lane % self.lanes.len();
        self.next_lane = self.next_lane.wrapping_add(1);
        self.queue_for(lane)
    }

    pub fn queue_for(&mut self, lane: usize) -> Queue<'_, 'pool, IOV, B> {
        let Self {
            lanes,
            entries,
            wire,
            ..
        } = self;
        lanes[lane].queue(entries, wire)
    }
}

impl<B, const IOV: usize> Drop for Arena<'_, B, IOV> {
    fn drop(&mut self) {
        for lane in &mut self.lanes {
            lane.clear(&self.entries);
        }
    }
}

pub(super) struct PreparedChain<'a, B> {
    pool: &'a MetadataPool<Entry<B>>,
    head: ReservedIndex,
    tail: ReservedIndex,
    entries: usize,
    bytes: usize,
    resident: usize,
}

impl<'a, B: AsRef<[u8]>> PreparedChain<'a, B> {
    pub(super) fn new(pool: &'a MetadataPool<Entry<B>>) -> Self {
        Self {
            pool,
            head: ReservedIndex::NONE,
            tail: ReservedIndex::NONE,
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

    fn link(&mut self, index: ReservedIndex, bytes: usize, resident: usize) -> bool {
        let Some(total_bytes) = self.bytes.checked_add(bytes) else {
            drop(self.pool.take_reserved(index));
            return false;
        };
        let Some(total_resident) = self.resident.checked_add(resident) else {
            drop(self.pool.take_reserved(index));
            return false;
        };
        if self.tail.is_none() {
            self.head = index;
        } else {
            self.pool.set_reserved_next(self.tail, index);
        }
        self.tail = index;
        self.entries += 1;
        self.bytes = total_bytes;
        self.resident = total_resident;
        true
    }

    pub(super) fn commit(mut self, queue: &MetadataQueue<'_, Entry<B>>) -> bool {
        if !queue.commit_prepared(
            self.head,
            self.tail,
            self.entries,
            self.bytes,
            self.resident,
        ) {
            return false;
        }
        self.head = ReservedIndex::NONE;
        true
    }
}

impl<B> Drop for PreparedChain<'_, B> {
    fn drop(&mut self) {
        self.pool.drain_reserved(&mut self.head);
    }
}
