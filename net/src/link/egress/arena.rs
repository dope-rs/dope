use std::pin::Pin;

use o3::buffer::Shared;
use o3::cell::RegionToken;

use super::EGRESS_QUANTUM;
use super::StableBytes;
use super::config::Config;
use super::entry::{Entry, PreparedEntry};
use super::metadata;
use super::metadata::pool::{Pool, ReservedIndex};
use super::queue::{Queue, QueueState};
use super::storage::Storage;
use super::wire;

pub struct Arena<'d, 'pool, B = Shared, const IOV: usize = 32> {
    lanes: Pin<Box<[QueueState<'pool, IOV>]>>,
    entries: Pool<'d, Entry<B>>,
    wire: wire::Arena<'pool>,
    next_lane: usize,
}

impl<'d, 'pool, B, const IOV: usize> Arena<'d, 'pool, B, IOV> {
    pub fn with_config(
        storage: &'pool Storage,
        token: &RegionToken<'d>,
        config: Config,
        lanes: usize,
    ) -> Self {
        assert!(lanes != 0, "egress arena requires at least one lane");
        assert_eq!(
            storage.config.wire_bytes(),
            config.wire_bytes(),
            "egress storage and arena wire capacities must match"
        );
        Self {
            lanes: Box::into_pin(
                (0..lanes)
                    .map(|lane| QueueState::with_config(config, lanes, lane))
                    .collect(),
            ),
            entries: Pool::with_config(token, config),
            wire: wire::Arena::new(&storage.wire),
            next_lane: 0,
        }
    }

    #[must_use]
    pub fn clear(&mut self, token: &mut RegionToken<'d>, lane: usize) -> bool {
        // SAFETY: This lane lives in the pinned allocation.
        unsafe { Pin::new_unchecked(&mut self.lanes.as_mut().get_unchecked_mut()[lane]) }
            .clear(&self.entries, token)
    }
}

impl<'d, 'pool, B: StableBytes, const IOV: usize> Arena<'d, 'pool, B, IOV> {
    pub fn try_enqueue(&self, token: &mut RegionToken<'d>, lane: usize, bytes: B) -> Result<(), B> {
        Queue::<IOV, B>::enqueue(
            metadata::Queue::with_lane(&self.entries, &self.lanes[lane].metadata),
            token,
            bytes,
        )
    }

    pub fn bytes(&self, lane: usize) -> usize {
        metadata::Queue::with_lane(&self.entries, &self.lanes[lane].metadata).bytes()
    }

    pub fn queue(&mut self) -> Queue<'_, 'd, 'pool, IOV, B> {
        let lane = self.next_lane % self.lanes.len();
        self.next_lane = self.next_lane.wrapping_add(1);
        self.queue_for(lane)
    }

    pub fn queue_for(&mut self, lane: usize) -> Queue<'_, 'd, 'pool, IOV, B> {
        let Self {
            lanes,
            entries,
            wire,
            ..
        } = self;
        // SAFETY: This lane lives in the pinned allocation.
        unsafe { Pin::new_unchecked(&mut lanes.as_mut().get_unchecked_mut()[lane]) }
            .queue(entries, wire)
    }
}

pub(super) struct PreparedChain<'a, 'd, B> {
    pool: &'a Pool<'d, Entry<B>>,
    token: &'a mut RegionToken<'d>,
    head: ReservedIndex,
    tail: ReservedIndex,
    entries: usize,
    bytes: usize,
    resident: usize,
}

impl<'a, 'd, B: StableBytes> PreparedChain<'a, 'd, B> {
    pub(super) fn new(pool: &'a Pool<'d, Entry<B>>, token: &'a mut RegionToken<'d>) -> Self {
        Self {
            pool,
            token,
            head: ReservedIndex::NONE,
            tail: ReservedIndex::NONE,
            entries: 0,
            bytes: 0,
            resident: 0,
        }
    }

    pub(super) fn push(&mut self, value: B) -> bool {
        match Entry::prepare_buffer(self.pool, self.token, value) {
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

    pub(super) fn push_wire(&mut self, span: super::wire::Span) -> bool {
        match Entry::prepare_wire(self.pool, self.token, span) {
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
            match Entry::prepare_copy(self.pool, self.token, chunk) {
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
        match Entry::prepare_static(self.pool, self.token, src) {
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
            drop(self.pool.take_reserved(self.token, index));
            return false;
        };
        let Some(total_resident) = self.resident.checked_add(resident) else {
            drop(self.pool.take_reserved(self.token, index));
            return false;
        };
        if self.tail.is_none() {
            self.head = index;
        } else {
            self.pool.set_reserved_next(self.token, self.tail, index);
        }
        self.tail = index;
        self.entries += 1;
        self.bytes = total_bytes;
        self.resident = total_resident;
        true
    }

    pub(super) fn commit(mut self, queue: &metadata::Queue<'_, 'd, Entry<B>>) -> bool {
        if !queue.commit_prepared(
            self.token,
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

impl<B> Drop for PreparedChain<'_, '_, B> {
    fn drop(&mut self) {
        self.pool.drain_reserved(self.token, &mut self.head);
    }
}
