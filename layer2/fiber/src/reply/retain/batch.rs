//! Detached batch resource credit.

use std::mem;

use crate::reply::{
    retain::{grant::payload, pool},
    usage,
};

/// One linear owner for every value retained by a detached reply batch.
pub(crate) struct Batch<'arena, 'credits, 'region> {
    pub(super) edge: pool::LeaseEdge<'arena, 'credits, 'region>,
    pub(in crate::reply) sum: payload::raw::Sum<'credits>,
}

impl Batch<'_, '_, '_> {
    pub(crate) fn wake(&self) {
        self.edge.wake();
    }

    pub(in crate::reply) fn returned(&self, amount: payload::Value) {
        self.edge.returned(amount);
    }

    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        self.sum.usage()
    }
}

const _: () =
    assert!(mem::size_of::<Batch<'static, 'static, 'static>>() == 5 * mem::size_of::<usize>());
