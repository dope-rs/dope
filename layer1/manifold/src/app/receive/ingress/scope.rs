use std::{ops::ControlFlow, pin};

use dope_core::driver::{self, route::Identity, schedule};
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

    type Admission<'a, 'c, 'd>
        = ingress::Single<'a, 'c, 'd>
    where
        'c: 'a,
        'd: 'a + 'c,
        W: 'd;

    fn drive_data<'p, 'a, 'c, 'w, 'd, ID: Identity + 'd, P, A>(
        policy: pin::Pin<&mut P>,
        data: A,
        work: Self::Admission<'a, 'c, 'd>,
    ) where
        'c: 'a,
        'd: 'a + 'c + 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, input::Receipt<W, M>, Observed = event::Data<'d>>,
    {
        work.drive(
            policy,
            data,
            |prep| prep.scoped(),
            |mut policy, next, turn, driver| match next {
                event::DispatchRecv::Control(next) => {
                    ingress::apply_control(policy, next, turn, driver);
                }
                event::DispatchRecv::Chunk(key, chunk) => {
                    ingress::prepare_terminal(policy.as_mut(), key);
                    let done = P::receive(policy.as_mut(), key, chunk, turn.reborrow(), driver);
                    let _ = flow::settle(policy, key, done, turn, driver);
                }
            },
        )
    }

    fn resume_tail<'p, 'w, 'd, ID: Identity + 'd, P>(
        mut policy: pin::Pin<&mut P>,
        item: &delivery::Resumed<'d, ID>,
        first: bool,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ControlFlow<(), bool>
    where
        'd: 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
    {
        let (key, chunk, work) = {
            let mut recv = P::storage(policy.as_mut()).recv();
            let Some(taken) = recv.take_tail(item)? else {
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
            (item.key(), taken.commit(), work)
        };
        let (driver, step) = work.into();
        ingress::prepare_terminal(policy.as_mut(), key);
        let done = P::receive(policy.as_mut(), key, chunk, step.reborrow(), driver);
        flow::settle(policy.as_mut(), key, done, step.reborrow(), driver)?;
        P::storage(policy).recv().finish_tail(item);
        ControlFlow::Continue(true)
    }
}
