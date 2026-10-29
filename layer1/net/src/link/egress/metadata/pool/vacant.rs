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

#[must_use = "a prepared metadata slot must be published or rolled back"]
pub(in crate::link::egress::metadata) struct Prepared<
    'queue,
    'p,
    'd,
    T,
    D: demand::Mode = demand::Fallible,
> {
    credit: credit::Bound<'queue, 'p, 'd, T, metadata::Local, D>,
    index: index::Index<'d>,
    value: egress::NonEmpty<T>,
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
        let (index, claim, value) = vacancy.reserve(token, claim, value);
        Self::new(claim, index, value)
    }
}

impl<'p, 'd, T, D: demand::Mode> Rollback<'d>
    for Vacant<'_, 'p, 'd, T, (), credit::Ready<'p, 'd>, D>
{
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        let Self {
            claim,
            index,
            value: (),
        } = self;
        let queue = claim.queue();
        queue.pool.store.recycle(token, index);
        claim.rollback(token);
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
        rest.rollback(token);
    }
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode>
    Vacant<'queue, 'p, 'd, T, egress::NonEmpty<T>, M, D>
{
    pub(in crate::link::egress) fn commit(self, token: &mut region::Token<region::Identity<'d>>) {
        let Self {
            claim,
            index,
            value,
        } = self;
        let len = value.geometry().wire();
        let (queue, stored) = credit::Bound::new(len, claim).into_parts();
        commit(queue, token, index, value, stored);
    }
}

impl<'queue, 'p, 'd, T, D: demand::Mode> Prepared<'queue, 'p, 'd, T, D> {
    pub(in crate::link::egress::metadata) fn from(
        node: Vacant<'queue, 'p, 'd, T, egress::NonEmpty<T>, metadata::Local, D>,
    ) -> Self {
        let Vacant {
            claim,
            index,
            value,
        } = node;
        let credit = credit::Bound::new(value.geometry().wire(), claim);
        Self {
            credit,
            index,
            value,
        }
    }

    pub(in crate::link::egress::metadata) const fn value(&self) -> &T {
        self.value.as_inner()
    }

    pub(in crate::link::egress::metadata) fn rollback(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> egress::NonEmpty<T> {
        recycle(self.credit, self.index, token);
        self.value
    }

    pub(in crate::link::egress::metadata) fn commit(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) {
        let Self {
            credit,
            index,
            value,
        } = self;
        let (queue, stored) = credit.into_parts();
        commit(queue, token, index, value, stored);
    }
}

impl<'p, 'd, T, D: demand::Mode> Rollback<'d> for Prepared<'_, 'p, 'd, T, D> {
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        recycle(self.credit, self.index, token);
    }
}

fn recycle<'p, 'd, T>(
    credit: credit::Bound<'_, 'p, 'd, T, metadata::Local, impl demand::Mode>,
    index: index::Index<'d>,
    token: &mut region::Token<region::Identity<'d>>,
) {
    let (queue, stored) = credit.into_parts();
    queue.pool.store.recycle(token, index);
    credit::Lane::new(queue).release(token, stored.into_grant());
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

const _: () =
    assert!(mem::size_of::<Prepared<'static, 'static, 'static, ()>>() == 7 * mem::size_of::<u64>());

const _: () = assert!(
    mem::size_of::<Bound<'static, 'static, 'static, (), demand::Fallible>>()
        == 8 * mem::size_of::<usize>()
);
