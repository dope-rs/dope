use std::{mem, pin};

use dope_core::driver::{
    receive::Stale,
    retained,
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

pub(crate) struct Single<'a, 'c, 'owner, 'd: 'owner> {
    driver: &'a mut retained::Context<'c, 'owner, 'd>,
    turn: schedule::Turn<'a, 'd>,
}

impl<'a, 'c: 'a, 'owner: 'a, 'd: 'a + 'c + 'owner> Admission<'a, 'c, 'owner, 'd>
    for Single<'a, 'c, 'owner, 'd>
{
    fn fresh(
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> schedule::Capacity<Self> {
        schedule::Capacity::Available(Self { driver, turn })
    }

    fn replay(
        replay: Replay,
        ready: ready::Key<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
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

impl<'a, 'c, 'owner, 'd: 'owner> Single<'a, 'c, 'owner, 'd> {
    pub(in crate::app::receive::ingress) fn drive<'p, 'w, ID: Identity + 'd, W, M, P, A, C>(
        self,
        mut policy: pin::Pin<&mut P>,
        data: A,
        emit: impl FnOnce(delivery::Data<'_, 'w, 'd, ID, W, M::Kind>) -> event::DispatchRecv<'d, ID, C>,
        apply: impl FnOnce(
            pin::Pin<&mut P>,
            event::DispatchRecv<'d, ID, C>,
            schedule::Turn<'_, 'd>,
            &mut retained::Context<'_, '_, 'd>,
        ),
    ) where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner + 'w + 'p,
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

impl<'a, 'c, 'owner, 'd: 'owner> From<Single<'a, 'c, 'owner, 'd>>
    for (
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    )
{
    fn from(work: Single<'a, 'c, 'owner, 'd>) -> Self {
        (work.driver, work.turn)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Single<'static, 'static, 'static, 'static>>() == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::align_of::<Single<'static, 'static, 'static, 'static>>() == mem::align_of::<usize>()
    );
};
