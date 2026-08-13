use o3::{buffer::storage, cell::region};

use crate::link::egress::{data, metadata};

mod counters;
pub(super) mod entry;
pub(in crate::link) mod flight;
pub(in crate::link::egress) mod lanes;
mod transfer;

pub struct Queue<'a, 'd, const IOV: usize, B = storage::Shared> {
    entries: metadata::Queue<'a, 'd, entry::Entry<B>>,
    counters: &'a counters::Counters,
    flight: flight::State<'a, 'd, IOV>,
}

impl<'a, 'd, const IOV: usize, B: data::Payload<'d>> Queue<'a, 'd, IOV, B> {
    pub(in crate::link) fn transfer(&mut self) -> transfer::Transfer<'_, 'a, 'd, IOV, B> {
        transfer::Transfer::new(self)
    }

    pub fn reborrow(&mut self) -> Queue<'_, 'd, IOV, B> {
        Queue {
            entries: self.entries,
            counters: self.counters,
            flight: self.flight.reborrow(),
        }
    }

    pub fn try_enqueue(&self, token: &mut region::Token<'d>, bytes: B) -> Result<(), B> {
        Self::enqueue(self.entries, token, bytes)
    }

    pub fn try_enqueue_pair(
        &self,
        token: &mut region::Token<'d>,
        first: B,
        second: Option<B>,
    ) -> bool {
        let mut prepared = self.entries.prepare(token);
        if !prepare_entry(&mut prepared, first) {
            return false;
        }
        if let Some(second) = second
            && !prepare_entry(&mut prepared, second)
        {
            return false;
        }
        prepared.commit()
    }

    pub(in crate::link) fn prepare_flight(
        &mut self,
        token: &mut region::Token<'d>,
        bytes_cap: usize,
    ) -> Option<flight::Prepared<'_, 'd, IOV>> {
        use dope_core::io::transfer::MAX_BYTES;

        let mut index = self.entries.head();
        if index.is_none() {
            return None;
        }
        let mut flight = self.flight.begin()?;
        let cap = bytes_cap.min(MAX_BYTES);
        let mut first = true;
        while !index.is_none() {
            if flight.len() == IOV || flight.bytes() >= cap {
                break;
            }
            let offset = if first { self.counters.partial() } else { 0 };
            let next = index.next(self.entries.pool, token);
            let prepared = index
                .inspect(self.entries.pool, token, |entry| {
                    entry::Prepare::prepare(entry, offset, cap - flight.bytes())
                })
                .flatten();
            let Some(prepared) = prepared else {
                debug_assert!(false, "retained egress entry must remain addressable");
                break;
            };
            if !flight.push(prepared.iovec) {
                break;
            }
            first = false;
            if prepared.iovec.len() < prepared.available {
                break;
            }
            index = next;
        }
        if flight.is_empty() {
            drop(flight);
            return None;
        }
        Some(flight)
    }

    pub fn is_send_inflight(&self) -> bool {
        self.flight.is_active()
    }

    fn ack(&mut self, token: &mut region::Token<'d>, mut released: flight::Released<IOV>) -> bool {
        let n = released.bytes();
        if n > self.entries.bytes() {
            return false;
        }
        let mut left = n;
        while left > 0 {
            if !released.take_entry() {
                return false;
            }
            let Some((entry, front)) = self.entries.take_front(token) else {
                return false;
            };
            let remaining = front.bytes();
            if left >= remaining {
                left -= remaining;
                self.counters.set_partial(0);
                front.release();
            } else {
                front.restore_unchanged(entry);
                self.entries.consume_front_bytes(token, left);
                self.counters.set_partial(self.counters.partial() + left);
                left = 0;
            }
        }
        true
    }

    pub fn total_bytes(&self) -> usize {
        self.entries.bytes()
    }

    pub(super) fn enqueue(
        entries: metadata::Queue<'_, 'd, entry::Entry<B>>,
        token: &mut region::Token<'d>,
        bytes: B,
    ) -> Result<(), B> {
        let len = bytes.as_ref().len();
        if len == 0 {
            return Ok(());
        }
        let resident = bytes.resident_bytes();
        let reservation = entries.pool.reserve(token, bytes, len, resident)?;
        if !entries.try_acquire(1, resident) {
            return Err(reservation.rollback(entries.pool, token));
        }
        let index = reservation.install(entries.pool, token, entry::Entry::retained);
        entries.commit_acquired(token, index, index, 1, len, resident);
        Ok(())
    }
}

fn prepare_entry<'d, B: data::Payload<'d>>(
    prepared: &mut metadata::Prepared<'_, '_, 'd, entry::Entry<B>>,
    value: B,
) -> bool {
    let bytes = value.as_ref().len();
    if bytes == 0 {
        drop(value);
        return true;
    }
    let resident = value.resident_bytes();
    match prepared.try_push_mapped(value, bytes, resident, entry::Entry::retained) {
        Ok(()) => true,
        Err(value) => {
            drop(value);
            false
        }
    }
}
