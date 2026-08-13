use std::{mem, process};

use crate::link::egress::{
    self, data,
    metadata::{self, pool},
    queue::{counters, entry, flight},
};

pub(in crate::link) struct Lane<'d, const IOV: usize> {
    pub(in crate::link) metadata: metadata::Lane<'d>,
    counters: counters::Counters,
    flight: Option<flight::Lease<'d, IOV>>,
}

const _: () = assert!(mem::size_of::<Lane<'static, 32>>() <= 128);

impl<'d, const IOV: usize> Lane<'d, IOV> {
    pub(in crate::link::egress) fn new(credit: pool::CreditState) -> Self {
        Self {
            metadata: metadata::Lane::new(credit),
            counters: counters::Counters::new(),
            flight: None,
        }
    }

    pub(in crate::link::egress) fn is_active(&self) -> bool {
        self.flight.is_some()
    }

    pub(in crate::link::egress) fn queue<'a, B: data::Payload<'d>>(
        &'a mut self,
        entries: &'a pool::Pool<'d, entry::Entry<B>>,
        flights: &'a mut flight::Pool<'d, IOV>,
    ) -> egress::Queue<'a, 'd, IOV, B> {
        egress::Queue {
            entries: metadata::Queue::with_lane(entries, &self.metadata),
            counters: &self.counters,
            flight: flight::State::new(&mut self.flight, flights),
        }
    }
}

impl<const IOV: usize> Drop for Lane<'_, IOV> {
    fn drop(&mut self) {
        if !self.metadata.is_empty() {
            process::abort();
        }
    }
}
