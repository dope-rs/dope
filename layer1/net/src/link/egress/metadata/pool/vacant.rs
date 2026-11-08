use std::mem;

use o3::base::cell::region;

use crate::link::egress::{
    self,
    load::credit,
    metadata::{
        self,
        pool::store::{self, index},
    },
    reserve::{demand, guard::Rollback},
};

#[must_use = "a vacant metadata slot must be published or rolled back"]
pub(in crate::link::egress) struct Vacant<
    'queue,
    'p,
    'd,
    T,
    V,
    M = metadata::Local,
    D: demand::Mode = demand::Fallible,
> where
    M: credit::Access<'p, 'd>,
{
    claim: credit::Claim<'queue, 'p, 'd, T, M, D>,
    index: index::Index<'d>,
    value: V,
}

impl<'queue, 'p, 'd, T, V, M: credit::Access<'p, 'd>, D: demand::Mode>
    Vacant<'queue, 'p, 'd, T, V, M, D>
{
    pub(in crate::link::egress) fn new(
        claim: credit::Claim<'queue, 'p, 'd, T, M, D>,
        index: index::Index<'d>,
        value: V,
    ) -> Self {
        Self {
            claim,
            index,
            value,
        }
    }

    pub(in crate::link::egress) fn prepare(
        vacancy: store::Vacancy<'queue, 'p, 'd, T>,
        claim: credit::Claim<'queue, 'p, 'd, T, M, D>,
        token: &mut region::Token<region::Identity<'d>>,
        value: V,
    ) -> Self {
        let index = vacancy.reserve(token);
        Self::new(claim, index, value)
    }

    pub(in crate::link::egress::metadata) fn reclaim(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> V {
        let Self {
            claim,
            index,
            value,
        } = self;
        let queue = claim.queue();
        queue.pool.store.recycle(token, index);
        claim.rollback(token);
        value
    }
}

impl<'p, 'd, T, V, M: credit::Access<'p, 'd>, D: demand::Mode> Rollback<'d>
    for Vacant<'_, 'p, 'd, T, V, M, D>
{
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        drop(self.reclaim(token));
    }
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode>
    Vacant<'queue, 'p, 'd, T, (), M, D>
{
    pub(in crate::link::egress) fn replace(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        value: egress::NonEmpty<T>,
    ) {
        Vacant::new(self.claim, self.index, value).commit(token);
    }
}

pub(in crate::link::egress) struct Bound<'queue, 'p, 'd, B, D: demand::Mode> {
    credit: credit::Part<'queue, 'p, 'd, B, B, credit::Ready<'p, 'd>, D>,
    index: index::Index<'d>,
}

impl<'queue, 'p, 'd, B, D: demand::Mode> Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D> {
    pub(in crate::link::egress) fn try_bind(
        self,
        load: egress::NonEmpty<B>,
    ) -> Result<Bound<'queue, 'p, 'd, B, D>, (Self, egress::NonEmpty<B>, egress::Shortage)> {
        let Self {
            claim,
            index,
            value: (),
        } = self;
        match credit::Part::try_new(claim, load) {
            Ok(credit) => Ok(Bound { credit, index }),
            Err((claim, load, cause)) => Err((
                Self {
                    claim,
                    index,
                    value: (),
                },
                load,
                cause,
            )),
        }
    }
}

impl<'queue, 'p, 'd, B, D: demand::Mode> Bound<'queue, 'p, 'd, B, D> {
    pub(in crate::link::egress) fn into_parts(
        self,
    ) -> (
        Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>,
        egress::NonEmpty<B>,
    ) {
        let Self { credit, index } = self;
        let (claim, load) = credit.into_parts();
        (
            Vacant {
                claim,
                index,
                value: (),
            },
            load,
        )
    }

    pub(in crate::link::egress) fn publish(self, token: &mut region::Token<region::Identity<'d>>) {
        let Self { credit, index } = self;
        let (rest, stored, value) = credit.take();
        commit(rest.queue(), token, index, value, stored);
        if rest.amount() != credit::Amount::ZERO {
            rest.rollback(token);
        }
    }
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode>
    Vacant<'queue, 'p, 'd, T, egress::NonEmpty<T>, M, D>
{
    pub(in crate::link::egress::metadata) const fn value(&self) -> &T {
        self.value.as_inner()
    }

    pub(in crate::link::egress) fn commit(self, token: &mut region::Token<region::Identity<'d>>) {
        let Self {
            claim,
            index,
            value,
        } = self;
        let len = value.geometry().wire();
        let (queue, stored) = claim.bind(len);
        commit(queue, token, index, value, stored);
    }
}

fn commit<'p, 'd, T>(
    queue: metadata::Queue<'_, 'p, 'd, T, impl credit::Access<'p, 'd>, impl demand::Mode>,
    token: &mut region::Token<region::Identity<'d>>,
    index: index::Index<'d>,
    value: egress::NonEmpty<T>,
    stored: credit::Stored<'p>,
) {
    queue.state().push(index, stored, |prev, entry| {
        queue.pool.store.write(token, index, value, entry);
        queue.pool.store.set_next(token, index, None);
        if let Some(prev) = prev {
            queue.pool.store.set_next(token, prev, Some(index));
        }
    });
}

const _: () = assert!(
    mem::size_of::<Vacant<'static, 'static, 'static, (), egress::NonEmpty<()>>>()
        == 7 * mem::size_of::<u64>()
);

const _: () = assert!(
    mem::size_of::<Bound<'static, 'static, 'static, (), demand::Fallible>>()
        == 8 * mem::size_of::<usize>()
);
