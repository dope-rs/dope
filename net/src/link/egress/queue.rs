use std::cell::Cell;
use std::marker::PhantomPinned;
use std::pin::Pin;

use dope_core::driver::token::Token;
use o3::buffer::Shared;
use o3::cell::RegionToken;

use super::StableBytes;
use super::WireLease;
use super::arena::PreparedChain;
use super::config::Config;
use super::entry::{Entry, PreparedEntry};
use super::flight::Flight;
use super::metadata::pool::Pool;
use super::metadata::{self, Lane};
use super::stage::Stage;
use super::wire;
use crate::wire::send::Vectored;

pub(super) struct QueueState<'pool, const IOV: usize> {
    lease: Option<WireLease<'pool>>,
    pub(super) metadata: Lane,
    partial_sent: Cell<u32>,
    submitted_plain: Cell<u32>,
    wire_base: u32,
    flight: Flight<IOV>,
    _pin: PhantomPinned,
}

impl<'pool, const IOV: usize> QueueState<'pool, IOV> {
    pub(super) fn with_config(config: Config, lanes: usize, lane: usize) -> Self {
        Self {
            lease: None,
            metadata: Lane::with_config(config, lanes, lane),
            partial_sent: Cell::new(0),
            submitted_plain: Cell::new(0),
            wire_base: 0,
            flight: Flight::new(),
            _pin: PhantomPinned,
        }
    }

    pub(super) fn clear<'d, B>(
        self: Pin<&mut Self>,
        entries: &Pool<'d, Entry<B>>,
        token: &mut RegionToken<'d>,
    ) -> bool {
        // SAFETY: Active flights are rejected before this pinned projection.
        let this = unsafe { self.get_unchecked_mut() };
        if this.flight.is_active() {
            return false;
        }
        this.lease.take();
        this.wire_base = 0;
        this.partial_sent.set(0);
        this.submitted_plain.set(0);
        drop(metadata::Queue::with_lane(entries, &this.metadata).detach_all(token));
        true
    }

    pub(super) fn queue<'a, 'd, B: StableBytes>(
        self: Pin<&'a mut Self>,
        entries: &'a Pool<'d, Entry<B>>,
        wire: &'a wire::Arena<'pool>,
    ) -> Queue<'a, 'd, 'pool, IOV, B> {
        // SAFETY: Queue never moves its pinned Flight.
        let this = unsafe { self.get_unchecked_mut() };
        Queue {
            entries: metadata::Queue::with_lane(entries, &this.metadata),
            wire: wire.state(&mut this.lease, &mut this.wire_base),
            partial_sent: &this.partial_sent,
            submitted_plain: &this.submitted_plain,
            flight: &mut this.flight,
        }
    }
}

pub struct Queue<'a, 'd, 'pool, const IOV: usize, B = Shared> {
    entries: metadata::Queue<'a, 'd, Entry<B>>,
    wire: wire::State<'a, 'pool>,
    partial_sent: &'a Cell<u32>,
    submitted_plain: &'a Cell<u32>,
    flight: &'a mut Flight<IOV>,
}

impl<'a, 'd, 'pool, const IOV: usize, B: StableBytes> Queue<'a, 'd, 'pool, IOV, B> {
    pub fn reborrow(&mut self) -> Queue<'_, 'd, 'pool, IOV, B> {
        Queue {
            entries: self.entries,
            wire: self.wire.reborrow(),
            partial_sent: self.partial_sent,
            submitted_plain: self.submitted_plain,
            flight: self.flight,
        }
    }

    pub fn try_enqueue(&self, token: &mut RegionToken<'d>, bytes: B) -> Result<(), B> {
        Self::enqueue(self.entries, token, bytes)
    }

    pub fn try_enqueue_pair(
        &self,
        token: &mut RegionToken<'d>,
        first: B,
        second: Option<B>,
    ) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool, token);
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

    pub(in crate::link) fn try_enqueue_static(
        &self,
        token: &mut RegionToken<'d>,
        bytes: &'static [u8],
    ) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool, token);
        prepared.push_static(bytes) && prepared.commit(&self.entries)
    }

    pub(in crate::link) fn prepare_flight(
        &mut self,
        token: &mut RegionToken<'d>,
        bytes_cap: usize,
    ) -> Option<Vectored<'_>> {
        if !self.begin_flight(token, bytes_cap) {
            return None;
        }
        Some(self.flight.vectored())
    }

    pub(in crate::link) fn mark_flight(&mut self, target: Token) {
        self.flight.mark(target);
    }

    pub(in crate::link) fn complete_flight(
        &mut self,
        token: &mut RegionToken<'d>,
        target: Token,
        bytes: usize,
    ) -> bool {
        if !self.flight.matches(target) {
            return false;
        }
        self.settle_flight(token, bytes)
    }

    pub(in crate::link) fn abort_flight(&mut self, target: Token) -> bool {
        if !self.flight.matches(target) {
            return false;
        }
        self.discard_flight()
    }

    pub(in crate::link) fn record_submitted_plain(&self, bytes: usize) -> bool {
        let Ok(bytes) = u32::try_from(bytes) else {
            return false;
        };
        let Some(total) = self.submitted_plain.get().checked_add(bytes) else {
            return false;
        };
        self.submitted_plain.set(total);
        true
    }

    pub(in crate::link) fn take_submitted_plain(&self) -> usize {
        self.submitted_plain.take() as usize
    }

    pub fn is_send_inflight(&self) -> bool {
        self.flight.is_active()
    }

    pub fn wire_stage<'stage>(
        &'stage mut self,
        token: &'stage mut RegionToken<'d>,
    ) -> Stage<'stage, 'd, 'pool, B> {
        if self.flight.has_wire() {
            return Stage::blocked(self.entries, token);
        }
        self.wire.stage(self.entries, token)
    }

    pub(in crate::link) fn try_enqueue_copy_pair(
        &mut self,
        token: &mut RegionToken<'d>,
        first: &[u8],
        second: Option<B>,
    ) -> bool {
        if first.is_empty() {
            return second.is_none_or(|second| self.try_enqueue(token, second).is_ok());
        }
        let mut prepared = PreparedChain::new(self.entries.pool, token);
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
        token: &mut RegionToken<'d>,
        first: &[u8],
        second: &'static [u8],
    ) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool, token);
        prepared.push_copy(first) && prepared.push_static(second) && prepared.commit(&self.entries)
    }

    fn begin_flight(&mut self, token: &mut RegionToken<'d>, bytes_cap: usize) -> bool {
        if self.flight.is_active() {
            return false;
        }
        self.flight.begin();
        let cap = bytes_cap.min(u32::MAX as usize);
        let mut index = self.entries.head();
        let mut first = true;
        while !index.is_none() {
            if self.flight.len() == IOV || self.flight.bytes() >= cap {
                break;
            }
            let offset = if first {
                self.partial_sent.get() as usize
            } else {
                0
            };
            let next = self.entries.pool.next(token, index);
            let wire_span = self
                .entries
                .pool
                .with_value(token, index, Entry::wire_span)
                .flatten();
            let prepared = if let Some(span) = wire_span {
                self.flight.saw_wire();
                self.wire.iov(span, offset, cap - self.flight.bytes())
            } else {
                self.entries
                    .pool
                    .with_value(token, index, |entry| {
                        Entry::<B>::iov(
                            entry.bytes().expect("non-wire entry"),
                            offset,
                            cap - self.flight.bytes(),
                        )
                    })
                    .flatten()
            };
            let Some((iov, available)) = prepared else {
                index = next;
                continue;
            };
            self.flight.push(iov);
            first = false;
            if iov.len() < available {
                break;
            }
            index = next;
        }
        if self.flight.is_empty() {
            self.flight.finish();
            return false;
        }
        true
    }

    pub(in crate::link) fn settle_flight(
        &mut self,
        token: &mut RegionToken<'d>,
        bytes: usize,
    ) -> bool {
        if !self.flight.is_active() || bytes > self.flight.bytes() {
            return false;
        }
        self.flight.finish();
        let acknowledged = self.try_ack(token, bytes);
        debug_assert!(acknowledged, "a completed flight is an ACK prefix");
        acknowledged
    }

    fn discard_flight(&mut self) -> bool {
        if !self.flight.is_active() {
            return false;
        }
        self.partial_sent.set(0);
        self.flight.finish();
        true
    }

    pub fn try_ack(&mut self, token: &mut RegionToken<'d>, n: usize) -> bool {
        if self.flight.is_active() {
            return false;
        }
        if n > self.entries.bytes() {
            return false;
        }
        let mut left = n;
        while left > 0 {
            let Some((entry, front)) = self.entries.take_front(token) else {
                return false;
            };
            let remaining = front.bytes();
            if left >= remaining {
                left -= remaining;
                self.partial_sent.set(0);
                let wire = entry.wire_span();
                front.release();
                if let Some(span) = wire
                    && !self.wire.try_consume(span)
                {
                    return false;
                }
            } else {
                front.restore(entry);
                self.entries.consume_front_bytes(token, left);
                self.partial_sent.set(self.partial_sent.get() + left as u32);
                left = 0;
            }
        }
        true
    }

    pub fn total_bytes(&self) -> usize {
        self.entries.bytes()
    }

    pub(super) fn enqueue(
        entries: metadata::Queue<'_, 'd, Entry<B>>,
        token: &mut RegionToken<'d>,
        bytes: B,
    ) -> Result<(), B> {
        match Entry::prepare_buffer(entries.pool, token, bytes)? {
            PreparedEntry::Empty => Ok(()),
            PreparedEntry::Node {
                index,
                bytes,
                resident,
            } => {
                if entries.commit_prepared(token, index, index, 1, bytes, resident) {
                    return Ok(());
                }
                let Some((_, entry, _, _)) = entries.pool.take_reserved(token, index) else {
                    unreachable!()
                };
                let Entry::Retained(value) = entry else {
                    unreachable!()
                };
                Err(value)
            }
        }
    }
}

impl<'d, const IOV: usize> Queue<'_, 'd, '_, IOV, Shared> {
    pub fn try_enqueue_all(&self, token: &mut RegionToken<'d>, frames: &[Shared]) -> bool {
        let mut prepared = PreparedChain::new(self.entries.pool, token);
        for frame in frames {
            if !prepared.push(frame.clone()) {
                return false;
            }
        }
        prepared.commit(&self.entries)
    }

    pub fn pending_at(&self, token: &RegionToken<'d>, idx: usize) -> Option<Shared> {
        let index = self.entries.index_at(token, idx)?;
        let bytes = self
            .entries
            .pool
            .with_value(token, index, |entry| entry.retained_ref().cloned())
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

#[cfg(test)]
mod tests {
    use dope_core::driver::token::{Epoch, SlotIndex, Token};
    use o3::buffer::Shared;
    use o3::cell::RegionToken;

    use super::super::storage::Storage;

    #[test]
    fn flight_retains_wire_until_its_matching_completion() {
        RegionToken::scope(|mut token| {
            let storage = Storage::default();
            let mut arena = storage.arena::<Shared, 8>(&token, 1);
            let mut queue = arena.queue();
            {
                let mut stage = queue.wire_stage(&mut token);
                stage.extend_from_slice(b"wire");
                assert_eq!(stage.commit(), 4);
            }

            let bytes: Vec<_> = {
                let flight = queue.prepare_flight(&mut token, usize::MAX).unwrap();
                flight
                    .iter()
                    .flat_map(|bytes| bytes.iter().copied())
                    .collect()
            };
            assert_eq!(bytes, b"wire");
            assert!(queue.is_send_inflight());
            {
                let stage = queue.wire_stage(&mut token);
                assert!(stage.overflowed());
                assert_eq!(stage.commit(), 0);
            }
            queue
                .try_enqueue(&mut token, Shared::copy_from_slice(b"next"))
                .unwrap();

            let target = Token::new(1, SlotIndex::ZERO, Epoch::INITIAL);
            queue.mark_flight(target);
            assert!(queue.complete_flight(&mut token, target, 4));
            assert_eq!(queue.total_bytes(), 4);
            assert!(queue.try_ack(&mut token, 4));
            assert_eq!(queue.total_bytes(), 0);
        });
    }
}
