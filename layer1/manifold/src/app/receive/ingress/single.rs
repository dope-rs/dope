use std::{mem, pin};

use dope_core::driver::{
    self,
    receive::Stale,
    route::Identity,
    schedule::{self, ready},
};
use dope_net::{
    link::{
        event,
        pool::recv::{arrival, delivery, input, reserve::Choice},
    },
    wire,
};

use crate::app::receive::ingress::{self, Admission, Dispatch, Replay};

pub(crate) struct Single<'a, 'c, 'd> {
    driver: &'a mut driver::Context<'c, 'd>,
    turn: schedule::Turn<'a, 'd>,
}

impl<'a, 'c: 'a, 'd: 'a + 'c> Admission<'a, 'c, 'd> for Single<'a, 'c, 'd> {
    fn fresh(
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut driver::Context<'c, 'd>,
    ) -> schedule::Capacity<Self> {
        schedule::Capacity::Available(Self { driver, turn })
    }

    fn replay(
        replay: Replay,
        ready: ready::Key<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut driver::Context<'c, 'd>,
    ) -> schedule::Capacity<Self> {
        if matches!(replay, Replay::Waiting) {
            let Some(permit) = turn.app().permit() else {
                driver.driver_ref().ready().activate(ready);
                return schedule::Capacity::Exhausted;
            };
            return permit.run(|| Self::fresh(turn, driver));
        }
        Self::fresh(turn, driver)
    }
}

impl<'a, 'c, 'd> Single<'a, 'c, 'd> {
    pub(in crate::app::receive::ingress) fn drive<'policy, 'p, 'w, ID: Identity + 'd, W, M, P, A, C>(
        self,
        policy: pin::Pin<&'policy mut P>,
        data: A,
        emit: impl FnOnce(
            delivery::Data<'_, 'w, 'd, ID, W, M::Kind, input::Receipt<W, M>>,
        ) -> event::DispatchRecv<'d, ID, C>,
        apply: impl FnOnce(
            &mut P::Context<'policy>,
            ingress::Entry<'policy, 'p, 'w, 'd, ID, P>,
            event::DispatchRecv<'d, ID, C>,
            schedule::Turn<'_, 'd>,
            &mut driver::Context<'_, 'd>,
        ) -> Option<ingress::Entry<'policy, 'p, 'w, 'd, ID, P>>,
    ) where
        'c: 'a,
        'd: 'a + 'c + 'w + 'p,
        W: wire::Spec + 'w + 'd,
        M: Dispatch<W>,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        crate::Inbound<'p, 'w, 'd, ID, P>: 'policy,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, input::Receipt<W, M>, Observed = event::Data<'d>>,
    {
        let (pool, mut context) = P::parts(policy);
        let (entry, plan) = pool.recv().data(
            data,
            |key, state| P::receipt(&mut context, key, state, self.driver.region_token()),
            |plan| plan.map(emit),
        );
        let Some(entry) = entry else { return };
        let next = match plan {
            event::Reservation::Ready(next) => next,
            event::Reservation::Control(next) => event::DispatchRecv::Control(next),
            event::Reservation::Parked(parked) => {
                let (driver, turn) = self.into();
                drop(ingress::apply_parked::<ID, P>(
                    &mut context, entry, parked, turn, driver,
                ));
                return;
            }
            event::Reservation::Stale(Stale) | event::Reservation::Drop => return,
        };
        let (driver, turn) = self.into();
        drop(apply(&mut context, entry, next, turn, driver));
    }
}

impl<'a, 'c, 'd> From<Single<'a, 'c, 'd>>
    for (&'a mut driver::Context<'c, 'd>, schedule::Turn<'a, 'd>)
{
    fn from(work: Single<'a, 'c, 'd>) -> Self {
        (work.driver, work.turn)
    }
}

const _: () = {
    assert!(mem::size_of::<Single<'static, 'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Single<'static, 'static, 'static>>() == mem::align_of::<usize>());
};
