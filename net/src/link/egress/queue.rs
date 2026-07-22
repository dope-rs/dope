use std::cell::Cell;

use o3::buffer::Shared;

use super::NONE;
use super::arena::PreparedChain;
use super::metadata::{MetadataArena, MetadataQueue};
use super::raw::entry::{Entry, PreparedEntry};
use super::stage::Stage;
use super::wire::WireArena;
use super::wire::raw::state::WireState;
use crate::wire::send::Vectored;
use dope_core::io::socket::msg::{IoVec, MsgHdr};

pub struct Queue<const IOV: usize, B = Shared> {
    entries: MetadataQueue<Entry<B>>,
    wire: WireState,
    partial_sent: Cell<u32>,
    iov_buf: [IoVec; IOV],
    iov_storage: [IoVec; IOV],
    msghdr_storage: MsgHdr,
}

impl<const IOV: usize, B: AsRef<[u8]>> Queue<IOV, B> {
    pub(super) fn with_arena(
        arena: &MetadataArena<Entry<B>>,
        wire: &WireArena,
        lane: usize,
    ) -> Self {
        Self {
            entries: MetadataQueue::new(arena, lane),
            wire: wire.state(),
            partial_sent: Cell::new(0),
            iov_buf: [IoVec::empty(); IOV],
            iov_storage: [IoVec::empty(); IOV],
            msghdr_storage: MsgHdr::empty(),
        }
    }

    pub fn try_enqueue(&self, bytes: B) -> Result<(), B> {
        match Entry::prepare_buffer(&self.entries.arena.pool, bytes)? {
            PreparedEntry::Empty => Ok(()),
            PreparedEntry::Node {
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
                let Some((_, entry, _, _)) = self.entries.arena.pool.take_node(index) else {
                    unreachable!()
                };
                let Entry::Retained { value, .. } = entry else {
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
        let wire = self.wire.prepare();
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
            let Some(prepared) = self
                .entries
                .arena
                .pool
                .with_value(index, |entry| entry.iov(offset, cap - bytes))
            else {
                break;
            };
            let Some((iov, available)) = prepared else {
                index = next;
                continue;
            };
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
                let wire_len = entry.wire_len();
                front.release();
                if let Some(len) = wire_len {
                    self.wire.consume(len);
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
        self.wire.clear();
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
        let Some(bytes) = self
            .entries
            .arena
            .pool
            .with_value(index, |entry| entry.retained_ref().cloned())
            .flatten()
        else {
            return Shared::new();
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
