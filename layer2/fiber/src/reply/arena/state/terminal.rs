//! Terminal reply-order transitions.

use crate::reply::{
    self,
    arena::{retire::Retired, state},
    order,
};

pub(in crate::reply::arena) enum Failure {
    Empty,
    Advanced,
    Drained(Retired),
}

pub(in crate::reply::arena) enum Completion {
    Complete(order::Depth),
    Drained {
        depth: order::Depth,
        retired: Retired,
    },
}

pub(in crate::reply::arena) struct Terminal<'a, 'lane, 'arena, 'credits, 'region: 'lane, D, E, K> {
    lane: reply::Lane<'lane, 'arena, 'credits, 'region, D, E, K>,
    state: &'a mut state::Book<'arena, 'credits, 'region, D, E, K>,
}

impl<'a, 'lane, 'arena, 'credits, 'region, D, E, K>
    Terminal<'a, 'lane, 'arena, 'credits, 'region, D, E, K>
{
    pub(in crate::reply::arena) const fn new(
        lane: reply::Lane<'lane, 'arena, 'credits, 'region, D, E, K>,
        state: &'a mut state::Book<'arena, 'credits, 'region, D, E, K>,
    ) -> Self {
        Self { lane, state }
    }

    pub(in crate::reply::arena) fn complete(&mut self) -> Completion {
        match state::Push::new(self.state, self.lane).head() {
            state::Head::Attached(open) => Completion::Complete(open.complete()),
            state::Head::Draining(order) => Completion::Complete(order.reclaim()),
            state::Head::Drained(order) => {
                let (depth, retired) = order.retire();
                match retired {
                    Some(retired) => Completion::Drained { depth, retired },
                    None => Completion::Complete(depth),
                }
            }
            state::Head::Empty(depth) => Completion::Complete(depth),
        }
    }

    pub(in crate::reply::arena) fn fail_one<C>(&mut self, cause: C) -> Option<order::Depth>
    where
        E: for<'cause> From<&'cause C>,
    {
        match state::Push::new(self.state, self.lane).head() {
            state::Head::Attached(open) => Some(open.fail_pop(&cause)),
            state::Head::Empty(_) | state::Head::Draining(_) | state::Head::Drained(_) => None,
        }
    }

    pub(in crate::reply::arena) fn fail<C>(&mut self, cause: &C) -> Failure
    where
        E: for<'cause> From<&'cause C>,
    {
        match state::Push::new(self.state, self.lane).head() {
            state::Head::Empty(_) => Failure::Empty,
            state::Head::Draining(order) => {
                order.take();
                Failure::Advanced
            }
            state::Head::Drained(order) => {
                order.retire().1.map_or(Failure::Advanced, Failure::Drained)
            }
            state::Head::Attached(open) => {
                open.fail_pop(cause);
                Failure::Advanced
            }
        }
    }
}
