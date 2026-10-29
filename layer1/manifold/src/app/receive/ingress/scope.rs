use std::{ops::ControlFlow, pin};

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::{
        event,
        pool::recv::{arrival, delivery, input, reserve::Choice},
    },
    wire,
    wire::receive::{Chunk, mode::Disabled},
};

use crate::app::receive::ingress::{
    self, Admission as _, Dispatch, Replay, Strategy, dispatch::flow,
};

pub(crate) struct Scope;

impl<W, M> Strategy<W, M> for Scope
where
    W: wire::Spec,
    M: Dispatch<W, Strategy = Scope>,
    M::Kind: delivery::ScopedData + input::Replay<W>,
{
    type Value<'input, 'w, 'd>
        = Chunk<'w, 'd, W, Disabled>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;

    type Pending<'w, 'd>
        = Chunk<'w, 'd, W, Disabled>
    where
        'd: 'w,
        W: 'w + 'd;

    type Admission<'a, 'c, 'owner, 'd>
        = ingress::Single<'a, 'c, 'owner, 'd>
    where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner,
        W: 'd;

    fn drive_data<'p, 'a, 'c, 'owner, 'w, 'd, ID: Identity + 'd, P, A>(
        policy: pin::Pin<&mut P>,
        data: A,
        work: Self::Admission<'a, 'c, 'owner, 'd>,
    ) where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner + 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        work.drive(
            policy,
            data,
            |prep| prep.scoped(),
            |policy, next, turn, driver| {
                flow::apply(
                    policy,
                    next,
                    turn,
                    driver,
                    |policy, key, input, turn, driver| {
                        P::receive(policy, key, M::from_value(input), turn, driver)
                    },
                )
            },
        )
    }

    fn resume_tail<'p, 'w, 'd, ID: Identity + 'd, P>(
        mut policy: pin::Pin<&mut P>,
        item: &delivery::Resumed<'d, ID>,
        first: bool,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> ControlFlow<(), bool>
    where
        'd: 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
    {
        let (key, chunk, work) = {
            let mut recv = P::storage(policy.as_mut()).recv();
            let Some((key, taken)) = recv.take_tail(item) else {
                return ControlFlow::Continue(false);
            };
            let replay = if first {
                Replay::First
            } else {
                Replay::Waiting
            };
            let work = match <Self as Strategy<W, M>>::Admission::replay(
                replay,
                item.ready(),
                turn.reborrow(),
                driver,
            ) {
                schedule::Capacity::Available(work) => work,
                schedule::Capacity::Exhausted => return ControlFlow::Break(()),
            };
            (key, taken.commit(), work)
        };
        let (driver, step) = work.into();
        ingress::prepare_terminal(policy.as_mut(), key);
        let done = P::receive(
            policy.as_mut(),
            key,
            M::from_value(chunk),
            step.reborrow(),
            driver,
        );
        flow::settle(policy.as_mut(), key, done, step.reborrow(), driver)?;
        P::storage(policy).recv().finish_tail(item);
        ControlFlow::Continue(true)
    }
}
