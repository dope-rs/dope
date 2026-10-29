use metadata::{batch::chain::Chain, pool::store::index::Index};
use o3::{base::cell::region, depot::arena::linked::fifo::ends};

use crate::link::egress::{
    self,
    load::credit,
    metadata::{self, batch},
    reserve::guard::{Guard, Rollback},
};

pub(in crate::link::egress) struct Draft<
    'queue,
    'token,
    'p,
    'd,
    T,
    M: credit::Access<'p, 'd> = metadata::Local,
> {
    links: ends::Exclusive<Option<Index<'d>>>,
    reservation: Guard<'token, 'd, credit::Reserve<'queue, 'p, 'd, T, M>>,
}

impl<'queue, 'token, 'p, 'd, T, M: credit::Access<'p, 'd>> Draft<'queue, 'token, 'p, 'd, T, M> {
    pub(in crate::link::egress) fn try_new(
        queue: metadata::Queue<'queue, 'p, 'd, T, M>,
        token: &'token mut region::Token<region::Identity<'d>>,
        plan: batch::Plan,
    ) -> Result<Self, egress::Admission> {
        let credit = queue.try_acquire(token, plan.amount())?;
        Ok(Self {
            links: ends::Exclusive::empty(),
            reservation: Guard::new(token, credit),
        })
    }

    pub(in crate::link::egress) fn try_build<R, E>(
        mut self,
        build: impl FnOnce(&mut Self) -> Result<R, E>,
    ) -> Result<R, batch::Error<E>> {
        let output = match build(&mut self) {
            Ok(output) => output,
            Err(error) => {
                self.cancel();
                return Err(batch::Error::Build(error));
            }
        };
        let amount = self.reservation.parts_mut().1.amount();
        if amount != credit::Amount::ZERO {
            let error = batch::Error::Mismatch {
                remaining: egress::Credit::new(amount.entries(), amount.resident()),
            };
            self.cancel();
            return Err(error);
        }
        let (token, credit) = self.reservation.into_parts();
        let queue = credit.queue();
        let mut links = Chain::new(self.links);
        queue.state().append(&mut links, |prev, source| {
            queue.pool.store.set_next(token, prev, Some(source));
        });
        Ok(output)
    }

    pub(in crate::link::egress) fn try_push(
        &mut self,
        load: egress::NonEmpty<T>,
    ) -> Result<(), batch::Rejected<T>> {
        let (token, credit) = self.reservation.parts_mut();
        let queue = credit.queue();
        let split = credit.try_split(load)?;
        let vacancy = match queue.pool.store.vacancy() {
            Some(vacancy) => vacancy,
            None => {
                let load = split.into_load();
                return Err(batch::Rejected {
                    reason: egress::Rejection::Capacity {
                        charge: load.charge(),
                        cause: egress::Admission::Storage {
                            lane: queue.lane,
                            requested: egress::Credit::new(
                                egress::Entries::new(1),
                                load.geometry().resident(),
                            ),
                            available: egress::Entries::new(0),
                        },
                    },
                    load,
                });
            }
        };
        let (stored, value) = split.take();
        let (index, stored, value) = vacancy.reserve(token, stored, value);
        queue.pool.store.write(token, index, value, stored);
        queue.pool.store.append(token, &mut self.links, index);
        Ok(())
    }

    fn cancel(mut self) {
        let (token, credit) = self.reservation.into_parts();
        let queue = credit.queue();
        if let Some(stored) = queue.pool.store.discard(token, &mut self.links) {
            let mut grant = stored.into_grant();
            while let Some(stored) = queue.pool.store.discard(token, &mut self.links) {
                grant.release();
                grant = stored.into_grant();
            }
            grant.release();
        }
        credit.rollback(token);
    }
}
