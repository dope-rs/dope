use o3::base::cell::region;

use crate::link::egress::{
    self,
    load::credit,
    metadata,
    reserve::{demand, guard::Rollback},
};

#[must_use = "a claimed slot must be published or released"]
#[repr(transparent)]
pub(in crate::link::egress) struct Claim<
    'queue,
    'p,
    'd,
    T,
    M = metadata::Local,
    D: demand::Mode = demand::Fallible,
> where
    M: credit::Access<'p, 'd>,
{
    pub(super) reserve: credit::Reserve<'queue, 'p, 'd, T, M, D>,
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode> Claim<'queue, 'p, 'd, T, M, D> {
    pub(in crate::link::egress) const fn from(
        reserve: credit::Reserve<'queue, 'p, 'd, T, M, D>,
    ) -> Self {
        Self { reserve }
    }

    pub(in crate::link::egress) fn try_acquire(
        queue: metadata::Queue<'queue, 'p, 'd, T, M, D>,
        token: &mut region::Token<region::Identity<'d>>,
        held: egress::Resident,
    ) -> Result<Self, egress::Admission> {
        let amount = credit::Amount::new(egress::Entries::new(1), held);
        Ok(Self {
            reserve: queue.try_acquire(token, amount)?,
        })
    }

    pub(in crate::link::egress) fn queue(&self) -> metadata::Queue<'queue, 'p, 'd, T, M, D> {
        self.reserve.queue()
    }

    pub(super) fn into_parts(
        self,
    ) -> (metadata::Queue<'queue, 'p, 'd, T, M, D>, credit::Grant<'p>) {
        self.reserve.into_parts()
    }
}

impl<'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode> Rollback<'d>
    for Claim<'_, 'p, 'd, T, M, D>
{
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        self.reserve.rollback(token);
    }
}

const _: () = assert!(
    std::mem::size_of::<Claim<'static, 'static, 'static, ()>>()
        == std::mem::size_of::<credit::Reserve<'static, 'static, 'static, ()>>()
);
