use data::compose;
use metadata::{
    Reserve,
    pool::{self, store},
};
use o3::base::cell::region;

use crate::link::egress::{
    self, data,
    load::credit,
    metadata,
    reserve::{demand, error, guard::Guard},
};

/// Queue-bound admission whose payload branch is chosen only at commit.
#[must_use = "a prepared branch must be committed or deliberately dropped"]
pub(in crate::link::egress) struct Prepared<
    'queue,
    'token,
    'p,
    'd,
    T,
    S,
    M: credit::Access<'p, 'd> = metadata::Local,
    D: demand::Mode = demand::Fallible,
> {
    claim: Guard<'token, 'd, credit::Claim<'queue, 'p, 'd, T, M, D>>,
    vacancy: store::Vacancy<'queue, 'p, 'd, T>,
    load: egress::NonEmpty<S>,
}

impl<'queue, 'p, 'd, L, R, M: credit::Access<'p, 'd>, D: demand::Mode>
    Prepared<'queue, '_, 'p, 'd, compose::Either<L, R>, L, M, D>
{
    pub(super) fn left(self) {
        let (token, claim) = self.claim.into_parts();
        pool::Vacant::prepare(self.vacancy, claim, token, self.load.into_left()).commit(token);
    }
}

impl<'queue, 'p, 'd, L, R, M: credit::Access<'p, 'd>, D: demand::Mode>
    Prepared<'queue, '_, 'p, 'd, compose::Either<L, R>, R, M, D>
{
    pub(in crate::link::egress) fn right(self) {
        let (token, claim) = self.claim.into_parts();
        pool::Vacant::prepare(self.vacancy, claim, token, self.load.into_right()).commit(token);
    }
}

impl<'queue, 'p, 'd, B, M: credit::Access<'p, 'd>, D: demand::Mode>
    Prepared<'queue, '_, 'p, 'd, B, B, M, D>
{
    pub(in crate::link::egress) fn retain(self) {
        let (token, claim) = self.claim.into_parts();
        pool::Vacant::prepare(self.vacancy, claim, token, self.load).commit(token);
    }
}

impl<'queue, 'token, 'p, 'd, T, D: demand::Mode>
    Prepared<'queue, 'token, 'p, 'd, T, T, metadata::Local, D>
{
    pub(super) fn reserve(self) -> Reserve<'queue, 'token, 'p, 'd, T, D> {
        let (token, claim) = self.claim.into_parts();
        let node = pool::Vacant::prepare(self.vacancy, claim, token, self.load);
        Reserve::new(node, token)
    }
}

impl<'queue, 'token, 'p, 'd, T, S, M, D> Prepared<'queue, 'token, 'p, 'd, T, S, M, D>
where
    M: credit::Access<'p, 'd>,
    D: demand::Mode,
{
    pub(in crate::link::egress) fn prepare(
        queue: metadata::Queue<'queue, 'p, 'd, T, M, D>,
        token: &'token mut region::Token<region::Identity<'d>>,
        load: egress::NonEmpty<S>,
    ) -> Result<Self, error::Capacity<S>> {
        let resident = load.geometry().resident();
        let vacancy = match queue.pool.store.vacancy() {
            Some(vacancy) => vacancy,
            None => {
                return Err(error::Capacity {
                    load,
                    cause: egress::Admission::Storage {
                        lane: queue.lane,
                        requested: egress::Credit::new(egress::Entries::new(1), resident),
                        available: egress::Entries::new(0),
                    },
                });
            }
        };
        let claim = match credit::Claim::try_acquire(queue, token, resident) {
            Ok(claim) => claim,
            Err(cause) => return Err(error::Capacity { load, cause }),
        };
        Ok(Self {
            claim: Guard::new(token, claim),
            vacancy,
            load,
        })
    }
}
