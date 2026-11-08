//! Terminal reply-order transitions.

use crate::reply::{
    arena::{retire::Retired, state},
    order,
    usage::Footprint,
};

pub(in crate::reply::arena) enum Failure {
    Empty,
    Advanced,
    Drained(Retired),
}

impl<D: Footprint, E, K> state::Push<'_, '_, '_, '_, D, E, K> {
    pub fn fail_one<C>(self, cause: C) -> Option<order::Depth>
    where
        E: for<'cause> From<&'cause C>,
    {
        match self.head() {
            state::Head::Attached(open) => Some(open.fail_pop(&cause)),
            state::Head::Empty(_) | state::Head::Draining(_) | state::Head::Drained(_) => None,
        }
    }

    pub(in crate::reply::arena) fn fail<C>(self, cause: &C) -> Failure
    where
        E: for<'cause> From<&'cause C>,
    {
        match self.head() {
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
