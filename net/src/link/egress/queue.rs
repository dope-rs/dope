use std::cell::Cell;

use o3::buffer::Shared;

use super::WireLease;
use super::arena::PreparedChain;
use super::config::Config;
use super::metadata::raw::pool::MetadataPool;
use super::metadata::{MetadataLane, MetadataQueue};
use super::raw::entry::{Entry, PreparedEntry};
use super::raw::preparation::Preparation;
use super::stage::Stage;
use super::wire::{WireArena, WireState};
use crate::wire::send::Vectored;
use dope_core::io::socket::msg::{IoVec, MsgHdr};

pub(super) struct QueueState<'pool, const IOV: usize> {
    lease: Option<WireLease<'pool>>,
    pub(super) metadata: MetadataLane,
    partial_sent: Cell<u32>,
    iov_buf: [IoVec; IOV],
    iov_storage: [IoVec; IOV],
    msghdr_storage: MsgHdr,
}

impl<'pool, const IOV: usize> QueueState<'pool, IOV> {
    pub(super) fn with_config(config: Config, lanes: usize, lane: usize) -> Self {
        Self {
            lease: None,
            metadata: MetadataLane::with_config(config, lanes, lane),
            partial_sent: Cell::new(0),
            iov_buf: [IoVec::empty(); IOV],
            iov_storage: [IoVec::empty(); IOV],
            msghdr_storage: MsgHdr::empty(),
        }
    }

    pub(super) fn clear<B>(&mut self, entries: &MetadataPool<Entry<B>>) {
        self.lease.take();
        self.partial_sent.set(0);
        drop(MetadataQueue::with_lane(entries, &self.metadata).detach_all());
    }

    pub(super) fn queue<'a, B: AsRef<[u8]>>(
        &'a mut self,
        entries: &'a MetadataPool<Entry<B>>,
        wire: &'a WireArena<'pool>,
    ) -> Queue<'a, 'pool, IOV, B> {
        Queue::with_arena(
            MetadataQueue::with_lane(entries, &self.metadata),
            wire.state(&mut self.lease),
            &self.partial_sent,
            &mut self.iov_buf,
            &mut self.iov_storage,
            &mut self.msghdr_storage,
        )
    }
}

pub struct Queue<'a, 'pool, const IOV: usize, B = Shared> {
    entries: MetadataQueue<'a, Entry<B>>,
    wire: WireState<'a, 'pool>,
    partial_sent: &'a Cell<u32>,
    iov_buf: &'a mut [IoVec; IOV],
    iov_storage: &'a mut [IoVec; IOV],
    msghdr_storage: &'a mut MsgHdr,
}

impl<'a, 'pool, const IOV: usize, B: AsRef<[u8]>> Queue<'a, 'pool, IOV, B> {
    pub(super) fn with_arena(
        entries: MetadataQueue<'a, Entry<B>>,
        wire: WireState<'a, 'pool>,
        partial_sent: &'a Cell<u32>,
        iov_buf: &'a mut [IoVec; IOV],
        iov_storage: &'a mut [IoVec; IOV],
        msghdr_storage: &'a mut MsgHdr,
    ) -> Self {
        Self {
            entries,
            wire,
            partial_sent,
            iov_buf,
            iov_storage,
            msghdr_storage,
        }
    }

    pub fn reborrow(&mut self) -> Queue<'_, 'pool, IOV, B> {
        Queue {
            entries: self.entries,
            wire: self.wire.reborrow(),
            partial_sent: self.partial_sent,
            iov_buf: self.iov_buf,
            iov_storage: self.iov_storage,
            msghdr_storage: self.msghdr_storage,
        }
    }

    pub fn try_enqueue(&self, bytes: B) -> Result<(), B> {
        Self::enqueue(self.entries, bytes)
    }

    pub fn try_enqueue_pair(&self, first: B, second: Option<B>) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool);
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

    pub(in crate::link) fn try_enqueue_static(&self, bytes: &'static [u8]) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool);
        prepared.push_static(bytes) && prepared.commit(&self.entries)
    }

    pub fn prepare_send(&mut self, bytes_cap: usize) -> Vectored<'_> {
        let n = self.fill_iovs(bytes_cap);
        Preparation::new(&self.iov_buf[..n], self.iov_storage, self.msghdr_storage).prepare()
    }

    pub fn wire_stage(&mut self) -> Stage<'_, 'pool, B> {
        self.wire.stage(self.entries)
    }

    pub(in crate::link) fn try_enqueue_copy_pair(
        &mut self,
        first: &[u8],
        second: Option<B>,
    ) -> bool {
        if first.is_empty() {
            return second.is_none_or(|second| self.try_enqueue(second).is_ok());
        }
        let mut prepared = PreparedChain::new(self.entries.pool);
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

    pub(in crate::link) fn try_enqueue_copy_static(
        &mut self,
        first: &[u8],
        second: &'static [u8],
    ) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool);
        prepared.push_copy(first) && prepared.push_static(second) && prepared.commit(&self.entries)
    }

    fn fill_iovs(&mut self, bytes_cap: usize) -> usize {
        let cap = bytes_cap.min(u32::MAX as usize);
        let mut count = 0usize;
        let mut bytes = 0usize;
        let mut index = self.entries.head();
        let mut first = true;
        while !index.is_none() {
            if count == IOV || bytes >= cap {
                break;
            }
            let offset = if first {
                self.partial_sent.get() as usize
            } else {
                0
            };
            first = false;
            let next = self.entries.pool.next(index);
            let Some(prepared) = self
                .entries
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
        count
    }

    pub fn try_ack(&mut self, n: usize) -> bool {
        if n > self.entries.bytes() {
            return false;
        }
        let mut left = n;
        while left > 0 {
            let Some((entry, front)) = self.entries.take_front() else {
                return false;
            };
            let remaining = front.bytes();
            if left >= remaining {
                left -= remaining;
                self.partial_sent.set(0);
                let wire_len = entry.wire_len();
                front.release();
                if let Some(len) = wire_len
                    && !self.wire.try_consume(len)
                {
                    return false;
                }
            } else {
                front.restore(entry);
                self.entries.consume_front_bytes(left);
                self.partial_sent.set(self.partial_sent.get() + left as u32);
                left = 0;
            }
        }
        true
    }

    pub fn total_bytes(&self) -> usize {
        self.entries.bytes()
    }

    pub(super) fn enqueue(entries: MetadataQueue<'_, Entry<B>>, bytes: B) -> Result<(), B> {
        match Entry::prepare_buffer(entries.pool, bytes)? {
            PreparedEntry::Empty => Ok(()),
            PreparedEntry::Node {
                index,
                bytes,
                resident,
            } => {
                if entries.commit_prepared(index, index, 1, bytes, resident) {
                    return Ok(());
                }
                let Some((_, entry, _, _)) = entries.pool.take_reserved(index) else {
                    unreachable!()
                };
                let Entry::Retained { value, .. } = entry else {
                    unreachable!()
                };
                Err(value)
            }
        }
    }
}

impl<const IOV: usize> Queue<'_, '_, IOV, Shared> {
    pub fn try_enqueue_all(&self, frames: &[Shared]) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool);
        for frame in frames {
            if !prepared.push(frame.clone()) {
                return false;
            }
        }
        prepared.commit(&self.entries)
    }

    pub fn pending_at(&self, idx: usize) -> Option<Shared> {
        let index = self.entries.index_at(idx)?;
        let bytes = self
            .entries
            .pool
            .with_value(index, |entry| entry.retained_ref().cloned())
            .flatten()?;
        let offset = if idx == 0 {
            self.partial_sent.get() as usize
        } else {
            0
        };
        if offset >= bytes.len() {
            None
        } else {
            bytes.get(offset..)
        }
    }
}
