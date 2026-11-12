use o3::base::cell::region;

use crate::link::egress::{
    self,
    data::position,
    load::credit,
    metadata::{self, batch},
    reserve::{demand, guard::Rollback},
};

/// One queue-bound fair-credit acquisition.
#[must_use = "a credit reservation must be stored or returned"]
pub(in crate::link::egress) struct Reserve<
    'queue,
    'p,
    'd,
    T,
    M = metadata::Local,
    D: demand::Mode = demand::Fallible,
    P: position::State = (),
> where
    M: credit::Access<'p, 'd>,
{
    queue: metadata::Queue<'queue, 'p, 'd, T, M, D, P>,
    grant: credit::Grant<'p>,
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State>
    Reserve<'queue, 'p, 'd, T, M, D, P>
{
    pub(in crate::link::egress) const fn new(
        queue: metadata::Queue<'queue, 'p, 'd, T, M, D, P>,
        grant: credit::Grant<'p>,
    ) -> Self {
        Self { queue, grant }
    }

    pub(in crate::link::egress) fn amount(&self) -> credit::Amount {
        self.grant.amount()
    }

    pub(in crate::link::egress) fn queue(&self) -> metadata::Queue<'queue, 'p, 'd, T, M, D, P> {
        self.queue
    }

    pub(in crate::link::egress) fn try_split(
        &mut self,
        load: egress::NonEmpty<T>,
    ) -> Result<credit::Split<'_, 'p, T>, batch::Rejected<T>> {
        credit::Split::try_new(&mut self.grant, load)
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        metadata::Queue<'queue, 'p, 'd, T, M, D, P>,
        credit::Grant<'p>,
    ) {
        (self.queue, self.grant)
    }

    pub(in crate::link::egress) fn split_pair(&mut self, pair: credit::Pair) -> Self {
        // SAFETY: Pair is the two active bounds that minted this aggregate grant.
        let grant = unsafe { self.grant.bind_mut(pair) }.split(());
        Self::new(self.queue, grant)
    }
}

impl<'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State> Rollback<'d>
    for Reserve<'_, 'p, 'd, T, M, D, P>
{
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        credit::Lane::new(self.queue).release(token, self.grant);
    }
}

const _: () = assert!(
    std::mem::size_of::<Reserve<'static, 'static, 'static, ()>>()
        == 5 * std::mem::size_of::<usize>()
);
