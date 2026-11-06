use dope_core::{
    driver::{retained::flight, route::kind},
    io::transfer,
};
use o3::depot::queue::ring::inline;

use crate::file::appender::{Failure, Storage};

pub(in crate::file) enum InFlight<'p, 'd, const N: usize> {
    Submitted {
        pending: Pending<'p, N>,
        flight: flight::Flight<'d, kind::Write>,
    },
    Rejected {
        pending: Pending<'p, N>,
    },
}
pub(in crate::file) struct Pending<'p, const N: usize> {
    block: inline::Lease<'p, Vec<u8>, N>,
    remaining: transfer::Len,
}
pub(in crate::file) struct Queue<'p, 'd, const N: usize, const B: usize> {
    pub(super) storage: &'p Storage<N, B>,
    pub(super) current: Option<inline::Lease<'p, Vec<u8>, N>>,
    pub(super) in_flight: Option<InFlight<'p, 'd, N>>,
}
impl<const N: usize, const B: usize> Queue<'_, '_, N, B> {
    pub(super) fn discard(&mut self) {
        if let Some(current) = self.current.take() {
            release(current);
        }
        while let Some((block, ())) = self.storage.pool.pop_front() {
            release(block);
        }
    }
}

impl<const N: usize, const B: usize> Drop for Queue<'_, '_, N, B> {
    fn drop(&mut self) {
        let pending = self.in_flight.is_some()
            || !self.storage.pool.is_empty()
            || self
                .current
                .as_ref()
                .is_some_and(|block| !block.get().is_empty());
        if pending && self.storage.failure().is_none() {
            self.storage.failure.set(Some(Failure::Cancelled));
        }
        if let Some(active) = self.in_flight.take() {
            active.cancel().release();
        }
        self.discard();
    }
}

fn release<const N: usize>(mut block: inline::Lease<'_, Vec<u8>, N>) {
    block.get_mut().clear();
    block.release();
}
impl<'p, 'd, const N: usize> InFlight<'p, 'd, N> {
    pub(super) fn cancel(self) -> Pending<'p, N> {
        match self {
            Self::Submitted { pending, .. } | Self::Rejected { pending } => pending,
        }
    }
}
impl<'p, const N: usize> Pending<'p, N> {
    pub(super) fn new(block: inline::Lease<'p, Vec<u8>, N>) -> Self {
        let remaining = transfer::Len::prefix(block.get().len(), transfer::Len::MAX);
        Self { block, remaining }
    }
    pub(super) fn bytes(&self) -> &[u8] {
        self.block.get()
    }
    pub(super) fn release(self) {
        release(self.block);
    }
    pub(super) const fn remaining(&self) -> transfer::Len {
        self.remaining
    }
    pub(in crate::file) const fn is_complete(&self) -> bool {
        self.remaining.is_zero()
    }
    pub(in crate::file) fn advance(&mut self, amount: transfer::Len) {
        self.remaining = self.remaining.abs_diff(amount);
    }
}
