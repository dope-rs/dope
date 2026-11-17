use o3::base::cell::region;

use crate::link::egress::{
    self,
    data::position,
    load::credit,
    metadata,
    reserve::{demand, guard::Rollback},
};

#[must_use = "a claimed slot must be published or released"]
pub(in crate::link::egress) struct Claim<
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
    pub(super) queue: metadata::Queue<'queue, 'p, 'd, T, M, D, P>,
    pub(super) account: credit::Account<'p, 'd>,
    pub(super) resident: egress::Resident,
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State>
    Claim<'queue, 'p, 'd, T, M, D, P>
{
    pub(in crate::link::egress) const fn from(
        reserve: credit::Reserve<'queue, 'p, 'd, T, M, D, P>,
    ) -> Self {
        Self {
            queue: reserve.queue,
            account: reserve.account,
            resident: reserve.amount.resident(),
        }
    }

    pub(in crate::link::egress) fn try_acquire(
        queue: metadata::Queue<'queue, 'p, 'd, T, M, D, P>,
        token: &mut region::Token<region::Identity<'d>>,
        held: egress::Resident,
    ) -> Result<Self, egress::Admission> {
        let amount = credit::Amount::new(egress::Entries::new(1), held);
        Ok(Self::from(queue.try_acquire(token, amount)?))
    }

    pub(in crate::link::egress) fn queue(&self) -> metadata::Queue<'queue, 'p, 'd, T, M, D, P> {
        self.queue
    }
}

impl<'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State> Rollback<'d>
    for Claim<'_, 'p, 'd, T, M, D, P>
{
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        // SAFETY: Claim owns exactly one entry and this resident amount.
        unsafe {
            self.account
                .release(credit::Amount::new(egress::Entries::new(1), self.resident));
        }
        credit::Lane::new(self.queue).released(token);
    }
}

const _: () = assert!(
    std::mem::size_of::<Claim<'static, 'static, 'static, ()>>()
        == std::mem::size_of::<credit::Reserve<'static, 'static, 'static, ()>>()
);
