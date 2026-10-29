use dope_core::{
    driver::{
        result::{SubmitCause, SubmitError},
        retained::flight,
        route::{self, kind},
    },
    io::transfer,
};
use o3::depot::queue::ring::inline;

use crate::file::appender::{Failure, Storage, block::Block};

pub(in crate::file) enum InFlight<'p, 'd, const N: usize, const B: usize> {
    Submitted {
        pending: Pending<'p, N, B>,
        flight: flight::Flight<'d, kind::Write>,
    },
    Rejected {
        pending: Pending<'p, N, B>,
    },
}
pub(in crate::file) struct Pending<'p, const N: usize, const B: usize> {
    block: inline::Lease<'p, Block<B>, N>,
    length: transfer::Len,
    remaining: transfer::Len,
}
pub(in crate::file) struct Queue<'p, 'd, const N: usize, const B: usize> {
    pub(super) storage: &'p Storage<N, B>,
    pub(super) current: Option<inline::Lease<'p, Block<B>, N>>,
    pub(super) in_flight: Option<InFlight<'p, 'd, N, B>>,
    pub(super) closed: bool,
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
                .is_some_and(|block| !block.get().bytes.is_empty());
        if pending && self.storage.failure().is_none() {
            self.storage.failure.set(Some(Failure::Cancelled));
        }
        if let Some(active) = self.in_flight.take() {
            active.cancel().release();
        }
        self.discard();
    }
}

fn release<const N: usize, const B: usize>(mut block: inline::Lease<'_, Block<B>, N>) {
    block.get_mut().bytes.clear();
    block.release();
}
impl<'p, 'd, const N: usize, const B: usize> InFlight<'p, 'd, N, B> {
    pub(super) fn try_new(
        pending: Pending<'p, N, B>,
        result: Result<flight::Flight<'d, kind::Write>, SubmitError>,
    ) -> Result<Self, (Pending<'p, N, B>, Failure)> {
        match result {
            Ok(flight) => Ok(Self::Submitted { pending, flight }),
            Err(error) => match error.cause() {
                SubmitCause::Full => Ok(Self::Rejected { pending }),
                SubmitCause::IdentityExhausted => Err((pending, Failure::Submission(error))),
            },
        }
    }
    pub(super) fn complete(
        self,
        target: route::Erased<'d, kind::Write>,
    ) -> Result<Pending<'p, N, B>, Self> {
        match self {
            Self::Submitted { pending, flight } if flight.matches(target) => {
                drop(flight);
                Ok(pending)
            }
            active => Err(active),
        }
    }
    pub(super) fn cancel(self) -> Pending<'p, N, B> {
        match self {
            Self::Submitted { pending, flight } => {
                drop(flight);
                pending
            }
            Self::Rejected { pending } => pending,
        }
    }
}
impl<'p, const N: usize, const B: usize> Pending<'p, N, B> {
    pub(super) fn new(block: inline::Lease<'p, Block<B>, N>) -> Self {
        let length = transfer::Len::prefix(block.get().bytes.len(), transfer::Len::MAX);
        Self {
            block,
            length,
            remaining: length,
        }
    }
    pub(super) fn block(&self) -> &Block<B> {
        self.block.get()
    }
    pub(super) fn release(self) {
        release(self.block);
    }
    pub(in crate::file) const fn length(&self) -> transfer::Len {
        self.length
    }
    pub(in crate::file) const fn written(&self) -> transfer::Len {
        self.length.abs_diff(self.remaining)
    }
    pub(in crate::file) const fn is_complete(&self) -> bool {
        self.remaining.is_zero()
    }
    pub(in crate::file) fn advance(&mut self, amount: transfer::Len) -> Result<(), transfer::Len> {
        if amount > self.remaining {
            return Err(self.remaining);
        }
        self.remaining = self.remaining.abs_diff(amount);
        Ok(())
    }
}
