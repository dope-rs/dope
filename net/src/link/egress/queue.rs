use std::cell::Cell;
use std::ptr;
use std::slice;

use o3::buffer::Shared;
use o3::cell::RawCell;

use super::config::Config;
use super::metadata::{MetadataArena, MetadataPool, MetadataQueue};
use super::stage::Stage;
use super::wire::{WireArena, WireBuf};
use super::{EGRESS_CAP_BYTES, EGRESS_QUANTUM, NONE};
use crate::wire::send::Vectored;
use dope_core::io::socket::msg::{IoVec, MsgHdr};

pub(super) enum Entry<B> {
    Retained {
        value: B,
        data: *const u8,
        len: usize,
    },
    Wire {
        data: *const u8,
        len: usize,
    },
    Inline {
        data: [u8; EGRESS_QUANTUM],
        len: u16,
    },
    Static {
        data: *const u8,
        len: usize,
    },
}

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
        Queue::with_arena(&self.entries, &self.wire, lane)
    }
}

impl<B> Default for Arena<B> {
    fn default() -> Self {
        Self::with_config(Config::default(), 1)
    }
}

enum PreparedBuffer {
    Empty,
    Node {
        index: u32,
        bytes: usize,
        resident: usize,
    },
}

impl<B: AsRef<[u8]>> MetadataPool<Entry<B>> {
    fn prepare_buffer(&self, value: B) -> Result<PreparedBuffer, B> {
        let (index, bytes) = self.reserve_from(
            value,
            |value| Entry::Retained {
                value,
                data: ptr::null(),
                len: 0,
            },
            |slot| {
                let Entry::Retained { value, data, len } = slot else {
                    unreachable!()
                };
                let src = value.as_ref();
                *data = src.as_ptr();
                *len = src.len();
                *len
            },
        )?;
        self.set_sizes(index, bytes, bytes);
        if bytes == 0 {
            drop(self.take_node(index));
            return Ok(PreparedBuffer::Empty);
        }
        Ok(PreparedBuffer::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    fn prepare_wire(&self, data: *const u8, len: usize) -> Option<PreparedBuffer> {
        if len == 0 {
            return Some(PreparedBuffer::Empty);
        }
        let (index, bytes) = self
            .reserve_from(
                (data, len),
                |(data, len)| Entry::Wire { data, len },
                |_| len,
            )
            .ok()?;
        self.set_sizes(index, bytes, bytes);
        Some(PreparedBuffer::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    fn prepare_copy(&self, src: &[u8]) -> Option<PreparedBuffer> {
        debug_assert!(!src.is_empty() && src.len() <= EGRESS_QUANTUM);
        let (index, bytes) = self
            .reserve_from(
                src,
                |src| {
                    let mut data = [0; EGRESS_QUANTUM];
                    data[..src.len()].copy_from_slice(src);
                    Entry::Inline {
                        data,
                        len: src.len() as u16,
                    }
                },
                |entry| match entry {
                    Entry::Inline { len, .. } => *len as usize,
                    _ => unreachable!(),
                },
            )
            .ok()?;
        self.set_sizes(index, bytes, bytes);
        Some(PreparedBuffer::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    fn prepare_static(&self, src: &'static [u8]) -> Option<PreparedBuffer> {
        if src.is_empty() {
            return Some(PreparedBuffer::Empty);
        }
        let (index, bytes) = self
            .reserve_from(
                src,
                |src| Entry::Static {
                    data: src.as_ptr(),
                    len: src.len(),
                },
                |entry| match entry {
                    Entry::Static { len, .. } => *len,
                    _ => unreachable!(),
                },
            )
            .ok()?;
        self.set_sizes(index, bytes, 0);
        Some(PreparedBuffer::Node {
            index,
            bytes,
            resident: 0,
        })
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
        match self.pool.prepare_buffer(value) {
            Ok(PreparedBuffer::Empty) => true,
            Ok(PreparedBuffer::Node {
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
        match self.pool.prepare_wire(data, len) {
            Some(PreparedBuffer::Empty) => true,
            Some(PreparedBuffer::Node {
                index,
                bytes,
                resident,
            }) => self.link(index, bytes, resident),
            None => false,
        }
    }

    pub(super) fn push_copy(&mut self, src: &[u8]) -> bool {
        for chunk in src.chunks(EGRESS_QUANTUM) {
            match self.pool.prepare_copy(chunk) {
                Some(PreparedBuffer::Empty) => {}
                Some(PreparedBuffer::Node {
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
        match self.pool.prepare_static(src) {
            Some(PreparedBuffer::Empty) => true,
            Some(PreparedBuffer::Node {
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

pub struct Queue<const IOV: usize, B = Shared> {
    entries: MetadataQueue<Entry<B>>,
    wire: RawCell<Option<WireBuf>>,
    wire_arena: WireArena,
    partial_sent: Cell<u32>,
    iov_buf: [IoVec; IOV],
    iov_storage: [IoVec; IOV],
    msghdr_storage: MsgHdr,
}

impl<const IOV: usize, B: AsRef<[u8]>> Queue<IOV, B> {
    fn with_arena(arena: &MetadataArena<Entry<B>>, wire: &WireArena, lane: usize) -> Self {
        Self {
            entries: MetadataQueue::new(arena, lane),
            wire: RawCell::new(None),
            wire_arena: wire.clone(),
            partial_sent: Cell::new(0),
            iov_buf: [IoVec::empty(); IOV],
            iov_storage: [IoVec::empty(); IOV],
            msghdr_storage: MsgHdr::empty(),
        }
    }

    pub fn try_enqueue(&self, bytes: B) -> Result<(), B> {
        match self.entries.arena.pool.prepare_buffer(bytes)? {
            PreparedBuffer::Empty => Ok(()),
            PreparedBuffer::Node {
                index,
                bytes,
                resident,
            } => {
                if self
                    .entries
                    .commit_prepared(index, index, 1, bytes, resident)
                {
                    return Ok(());
                }
                let Some((_, Entry::Retained { value, .. }, _, _)) =
                    self.entries.arena.pool.take_node(index)
                else {
                    unreachable!()
                };
                Err(value)
            }
        }
    }

    pub fn try_enqueue_pair(&self, first: B, second: Option<B>) -> bool {
        let mut prepared = PreparedChain::new(&self.entries.arena.pool);
        if !prepared.push(first) {
            return false;
        }
        if let Some(second) = second
            && !prepared.push(second)
        {
            return false;
        }
        prepared.commit(&self.entries)
    }

    pub(crate) fn try_enqueue_static(&self, bytes: &'static [u8]) -> bool {
        let mut prepared = PreparedChain::new(&self.entries.arena.pool);
        prepared.push_static(bytes) && prepared.commit(&self.entries)
    }

    pub fn prepare_send(&mut self, bytes_cap: usize) -> Vectored<'_> {
        let n = self.fill_iovs(bytes_cap).len();
        let Self {
            iov_buf,
            iov_storage,
            msghdr_storage,
            ..
        } = self;
        Vectored::new(&iov_buf[..n], iov_storage, msghdr_storage)
    }

    pub fn wire_stage(&mut self) -> Stage<'_, B> {
        let wire = self.wire.get_mut();
        if wire.is_none() {
            *wire = self.wire_arena.acquire();
        }
        Stage::open(wire, &self.entries)
    }

    pub(crate) fn try_enqueue_copy_pair(&mut self, first: &[u8], second: Option<B>) -> bool {
        if first.is_empty() {
            return second.is_none_or(|second| self.try_enqueue(second).is_ok());
        }
        let mut prepared = PreparedChain::new(&self.entries.arena.pool);
        if !prepared.push_copy(first) {
            return false;
        }
        if let Some(second) = second
            && !prepared.push(second)
        {
            return false;
        }
        prepared.commit(&self.entries)
    }

    pub(crate) fn try_enqueue_copy_static(&mut self, first: &[u8], second: &'static [u8]) -> bool {
        let mut prepared = PreparedChain::new(&self.entries.arena.pool);
        prepared.push_copy(first) && prepared.push_static(second) && prepared.commit(&self.entries)
    }

    pub(crate) fn fill_iovs(&mut self, bytes_cap: usize) -> &[IoVec] {
        let cap = bytes_cap.min(u32::MAX as usize);
        let mut count = 0usize;
        let mut bytes = 0usize;
        let mut index = self.entries.head.get();
        let mut first = true;
        while index != NONE {
            if count == IOV || bytes >= cap {
                break;
            }
            let offset = if first {
                self.partial_sent.get() as usize
            } else {
                0
            };
            first = false;
            let next = self.entries.arena.pool.next(index);
            let Some(entry) = self.entries.arena.pool.value_ptr(index) else {
                break;
            };
            let (data, len) = match unsafe { entry.as_ref() } {
                Entry::Retained { data, len, .. } => (*data, *len),
                Entry::Wire { data, len } => (*data, *len),
                Entry::Inline { data, len } => (data.as_ptr(), *len as usize),
                Entry::Static { data, len } => (*data, *len),
            };
            let slice = unsafe { slice::from_raw_parts(data, len) };
            if offset >= slice.len() {
                index = next;
                continue;
            }
            let available = slice.len() - offset;
            let take = available.min(cap - bytes);
            let iov = IoVec::from_slice(&slice[offset..offset + take]);
            self.iov_buf[count] = iov;
            bytes += iov.len();
            count += 1;
            if iov.len() < available {
                break;
            }
            index = next;
        }
        &self.iov_buf[..count]
    }

    pub fn ack(&self, n: usize) {
        let mut left = n;
        while left > 0 {
            let Some((entry, front)) = self.entries.take_front() else {
                break;
            };
            let remaining = front.bytes();
            if left >= remaining {
                left -= remaining;
                self.partial_sent.set(0);
                front.release();
                if let Entry::Wire { len, .. } = entry {
                    unsafe {
                        self.wire.with_mut(|wire| {
                            if let Some(buffer) = wire.as_mut() {
                                buffer.consume(len);
                                if buffer.is_empty() {
                                    wire.take();
                                }
                            }
                        })
                    };
                }
            } else {
                front.restore(entry);
                self.entries.consume_front_bytes(left);
                self.partial_sent.set(self.partial_sent.get() + left as u32);
                left = 0;
            }
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.entries.bytes()
    }
}

impl<const IOV: usize, B> Drop for Queue<IOV, B> {
    fn drop(&mut self) {
        self.wire.get_mut().take();
    }
}

impl<const IOV: usize> Queue<IOV, Shared> {
    pub fn try_enqueue_all(&self, frames: &[Shared]) -> bool {
        let mut prepared = PreparedChain::new(&self.entries.arena.pool);
        for frame in frames {
            if !prepared.push(frame.clone()) {
                return false;
            }
        }
        prepared.commit(&self.entries)
    }

    pub fn pending_at(&self, idx: usize) -> Shared {
        let Some(index) = self.entries.index_at(idx) else {
            return Shared::new();
        };
        let Some(entry) = self.entries.arena.pool.value_ptr(index) else {
            return Shared::new();
        };
        let bytes = match unsafe { entry.as_ref() } {
            Entry::Retained { value, .. } => value.clone(),
            Entry::Wire { .. } | Entry::Inline { .. } | Entry::Static { .. } => {
                return Shared::new();
            }
        };
        let offset = if idx == 0 {
            self.partial_sent.get() as usize
        } else {
            0
        };
        if offset >= bytes.len() {
            Shared::new()
        } else {
            bytes.slice(offset..)
        }
    }
}
