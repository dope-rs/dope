//! Terminal slot state and its owned failure capability.

use std::mem;

use crate::reply::{
    Failed, Slot,
    recv::outcome,
    retain::{self, grant::payload},
};

pub(super) enum End<E> {
    Open,
    Complete,
    Failed(Failed<E>),
}

/// Exclusive slot capability whose terminal payload is proven failed.
#[repr(C)]
pub(in crate::reply) struct Fault<'a, 's, 'arena, 'credits, 'region, D, E> {
    credit: Credit<'a, 's, 'arena, 'credits, 'region, D, E>,
    failure: E,
}

struct Credit<'a, 's, 'arena, 'credits, 'region, D, E> {
    slot: Slot<'a, 's, 'arena, 'credits, 'region, D, E>,
    amount: payload::Failure,
}

impl<E> End<E> {
    pub(super) fn fail(&mut self, item: Failed<E>) {
        *self = Self::Failed(item);
    }

    pub(super) fn take_failure(&mut self) -> Option<Failed<E>> {
        match mem::replace(self, Self::Complete) {
            Self::Failed(item) => Some(item),
            end => {
                *self = end;
                None
            }
        }
    }
}

impl<'a, 's, 'arena, 'credits, 'region, D, E> Fault<'a, 's, 'arena, 'credits, 'region, D, E> {
    pub(super) fn observe(
        slot: Slot<'a, 's, 'arena, 'credits, 'region, D, E>,
    ) -> Option<outcome::Terminal<'a, 's, 'arena, 'credits, 'region, D, E>> {
        match mem::replace(&mut slot.state.end, End::Complete) {
            End::Open => {
                slot.state.end = End::Open;
                None
            }
            End::Complete => Some(outcome::Terminal::Complete(outcome::Completion { slot })),
            End::Failed(failure) => Some(outcome::Terminal::Failed(outcome::Fault {
                slot: Self {
                    credit: Credit {
                        slot,
                        amount: failure.amount,
                    },
                    failure: failure.value,
                },
            })),
        }
    }

    pub(in crate::reply) fn pop(&mut self) -> Option<D> {
        self.credit.slot.pop()
    }

    pub(in crate::reply) fn into_error(self) -> E {
        self.failure
    }
}

impl<D, E> Drop for Credit<'_, '_, '_, '_, '_, D, E> {
    fn drop(&mut self) {
        // SAFETY: the removed member and payload come from this exact owned slot.
        unsafe { retain::raw::Remove::remove(&mut self.slot.owners.payload, self.amount) }
            .release();
    }
}

const _: () = {
    assert!(mem::size_of::<End<()>>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<End<u64>>() == 4 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Fault<'static, 'static, 'static, 'static, 'static, u64, u64>>()
            == mem::size_of::<Slot<'static, 'static, 'static, 'static, 'static, u64, u64>>()
                + mem::size_of::<Failed<u64>>()
    );
};
