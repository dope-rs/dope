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
        pool::recv::{arrival, delivery, reserve::Choice},
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
    pub(in crate::app::receive::ingress) fn drive<'p, 'w, ID: Identity + 'd, W, M, P, A, C>(
        self,
        mut policy: pin::Pin<&mut P>,
        data: A,
        emit: impl FnOnce(delivery::Data<'_, 'w, 'd, ID, W, M::Kind>) -> event::DispatchRecv<'d, ID, C>,
        apply: impl FnOnce(
            pin::Pin<&mut P>,
            event::DispatchRecv<'d, ID, C>,
            schedule::Turn<'_, 'd>,
            &mut driver::Context<'_, 'd>,
        ),
    ) where
        'c: 'a,
        'd: 'a + 'c + 'w + 'p,
        W: wire::Spec + 'w + 'd,
        M: Dispatch<W>,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        let plan = P::storage(policy.as_mut()).reserve().data(data).map(emit);
        let next = match plan {
            event::Reservation::Ready(next) => next,
            event::Reservation::Control(next) => event::DispatchRecv::Control(next),
            event::Reservation::Parked(parked) => {
                let (driver, turn) = self.into();
                ingress::apply_parked(policy, parked, turn, driver);
                return;
            }
            event::Reservation::Stale(Stale) | event::Reservation::Drop => return,
        };
        let (driver, turn) = self.into();
        apply(policy.as_mut(), next, turn, driver);
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
