use o3::base::cell::region;

use crate::link::egress::{
    self,
    data::position,
    load::credit,
    metadata::{self, pool::store::index::Index},
    reserve::{
        demand,
        guard::{Guard, Rollback},
    },
};

pub(in crate::link::egress::metadata::front) struct Vacant<
    'queue,
    'token,
    'p,
    'd,
    T,
    M,
    D: demand::Mode,
    P: position::State = (),
> where
    M: credit::Access<'p, 'd>,
{
    parts: Guard<'token, 'd, Parts<'queue, 'p, 'd, T, M, D, P>>,
}

pub(super) struct Parts<
    'queue,
    'p,
    'd,
    T,
    M: credit::Access<'p, 'd>,
    D: demand::Mode,
    P: position::State = (),
> {
    pub(super) queue: metadata::Queue<'queue, 'p, 'd, T, M, D, P>,
    pub(super) slot: Index<'d>,
    pub(super) stored: credit::Stored<'p>,
}

impl<'queue, 'token, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State>
    Vacant<'queue, 'token, 'p, 'd, T, M, D, P>
{
    pub(in crate::link::egress::metadata::front) fn new(
        queue: metadata::Queue<'queue, 'p, 'd, T, M, D, P>,
        slot: Index<'d>,
        token: &'token mut region::Token<region::Identity<'d>>,
        stored: credit::Stored<'p>,
    ) -> Self {
        Self {
            parts: Guard::new(
                token,
                Parts {
                    queue,
                    slot,
                    stored,
                },
            ),
        }
    }

    pub(in crate::link::egress::metadata::front) fn region_token(
        &mut self,
    ) -> &mut region::Token<region::Identity<'d>> {
        self.parts.parts_mut().0
    }

    pub(in crate::link::egress::metadata::front) fn restore(self, value: egress::NonEmpty<T>) {
        let (
            token,
            Parts {
                queue,
                slot,
                stored,
            },
        ) = self.parts.into_parts();
        queue.state().restore(slot, stored, |next, entry| {
            queue.pool.store.restore(token, slot, value, next, entry);
        });
    }
}

impl<'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State> Rollback<'d>
    for Parts<'_, 'p, 'd, T, M, D, P>
{
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        let Parts {
            queue,
            slot,
            stored,
        } = self;
        queue.pool.store.recycle(token, slot);
        credit::Lane::new(queue).release(token, stored.into_grant());
    }
}
