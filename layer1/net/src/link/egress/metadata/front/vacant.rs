use o3::base::cell::region;

use crate::link::egress::{
    self,
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
> where
    M: credit::Access<'p, 'd>,
{
    parts: Guard<'token, 'd, Parts<'queue, 'p, 'd, T, M, D>>,
}

pub(super) struct Parts<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode> {
    pub(super) queue: metadata::Queue<'queue, 'p, 'd, T, M, D>,
    pub(super) slot: Index<'d>,
    pub(super) stored: credit::Stored<'p>,
}

impl<'queue, 'token, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode>
    Vacant<'queue, 'token, 'p, 'd, T, M, D>
{
    pub(in crate::link::egress::metadata::front) fn new(
        queue: metadata::Queue<'queue, 'p, 'd, T, M, D>,
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

    pub(in crate::link::egress::metadata::front) fn with_region<R>(
        &mut self,
        f: impl FnOnce(&mut region::Token<region::Identity<'d>>) -> R,
    ) -> R {
        f(self.parts.parts_mut().0)
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

impl<'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode> Rollback<'d>
    for Parts<'_, 'p, 'd, T, M, D>
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
