//! Scheduling and backpressure control for one reply lane.

use o3::base::cell::region;

use crate::reply::{Lane, Registration, arena::state::Register, retain::pool};

pub struct Control<'a, 'arena, 'credits, 'region: 'a, D, E = D, K = ()> {
    lane: Lane<'a, 'arena, 'credits, 'region, D, E, K>,
}

impl<D, E, K> Copy for Control<'_, '_, '_, '_, D, E, K> {}

impl<D, E, K> Clone for Control<'_, '_, '_, '_, D, E, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, 'arena, 'credits, 'region, D, E, K> Control<'a, 'arena, 'credits, 'region, D, E, K> {
    pub(super) const fn new(lane: Lane<'a, 'arena, 'credits, 'region, D, E, K>) -> Self {
        Self { lane }
    }

    pub fn activate(self, token: &mut region::Token<region::Identity<'region>>) {
        token.borrow_mut(&self.lane.arena.state).activate(self.lane);
    }

    pub fn deactivate(self, token: &mut region::Token<region::Identity<'region>>) {
        pool::Lane::new(self.lane).cancel();
        token
            .borrow_mut(&self.lane.arena.state)
            .order
            .deactivate(self.lane.index);
    }

    pub fn attachment(
        self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> crate::reply::Attachment {
        token
            .borrow(&self.lane.arena.state)
            .order
            .view(self.lane.index)
            .state()
            .attachment()
    }

    /// Borrows the ordered tag while its region remains borrowed.
    pub fn front_tag<'p>(self, token: &'p region::Token<region::Identity<'region>>) -> Option<&'p K>
    where
        'a: 'p,
    {
        token.borrow(&self.lane.arena.state).front_tag(self.lane)
    }

    pub fn registration(
        self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Registration {
        Register::new(token.borrow_mut(&self.lane.arena.state)).registration(self.lane)
    }
}
