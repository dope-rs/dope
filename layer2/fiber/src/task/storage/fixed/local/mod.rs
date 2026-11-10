use std::{ops::ControlFlow, pin, task};

use o3::depot::arena::slab::key;

use crate::{abi, context, task::storage::fixed::Id};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Removal {
    Removed,
    Absent,
}

pub trait Member {
    type Fiber;

    fn value(&mut self) -> pin::Pin<&mut Self::Fiber>;
    fn discard(self);
}

/// A terminal identity carries no polling or mutable access authority.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Ended(pub(in crate::task::storage::fixed) key::Parts);

impl From<Ended> for key::Parts {
    fn from(ended: Ended) -> Self {
        ended.0
    }
}

pub struct Entry<M, I> {
    pub(in crate::task::storage::fixed) member: M,
    pub(in crate::task::storage::fixed) id: I,
}

impl<M: Member, I> Entry<M, I> {
    pub fn get(&mut self) -> &M::Fiber {
        self.member.value().into_ref().get_ref()
    }

    pub fn into_id(self) -> I {
        self.id
    }

    pub fn remove(self) {
        self.member.discard();
    }
}

impl<'s, 'd, D, M: Member> Entry<M, Id<'s, 'd, D>> {
    pub fn into_parts(self) -> (Id<'s, 'd, D>, M) {
        (self.id, self.member)
    }

    #[inline(always)]
    pub fn poll_next(
        mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> ControlFlow<
        Entry<M, Id<'s, 'd, D, Ended>>,
        (Self, task::Poll<<M::Fiber as abi::Stream<'d>>::Item>),
    >
    where
        M::Fiber: abi::Stream<'d>,
    {
        match context::PollCall::try_poll(
            self.member.value(),
            context,
            <M::Fiber as abi::Stream<'d>>::poll_next,
        ) {
            context::PollResult::Polled(task::Poll::Ready(None)) => ControlFlow::Break(Entry {
                member: self.member,
                id: self.id.ended(),
            }),
            context::PollResult::Polled(task::Poll::Ready(Some(item))) => {
                ControlFlow::Continue((self, task::Poll::Ready(item)))
            }
            context::PollResult::Polled(task::Poll::Pending) | context::PollResult::Exhausted => {
                ControlFlow::Continue((self, task::Poll::Pending))
            }
        }
    }
}
