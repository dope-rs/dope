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
            |context, entry, next, turn, driver| match next {
                event::DispatchRecv::Control(next) => {
                    ingress::apply_control::<ID, P>(context, entry, next, turn, driver)
                }
                event::DispatchRecv::Chunk(_, chunk) => {
                    ingress::prepare_terminal::<ID, P>(&entry);
                    let (entry, done) = P::receive(context, entry, chunk, turn.reborrow(), driver);
                    let entry = entry?;
                    match flow::settle::<ID, P>(context, entry, done, turn, driver) {
                        ControlFlow::Break(entry) | ControlFlow::Continue(entry) => entry,
                    }
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
        let (pool, mut context) = P::parts(policy.as_mut());
        let Ok(mut entry) = pool.entry(item.key()) else {
            return ControlFlow::Continue(false);
        };
        let Some(taken) = entry.take_tail()? else {
            return ControlFlow::Continue(false);
        };
        let replay = if first { Replay::First } else { Replay::Waiting };
        let work = match <Self as Strategy<W, M>>::Admission::replay(
            replay, item.ready(), turn.reborrow(), driver,
        ) {
            schedule::Capacity::Available(work) => work,
            schedule::Capacity::Exhausted => return ControlFlow::Break(()),
        };
        let chunk = taken.commit();
        let (driver, step) = work.into();
        ingress::prepare_terminal::<ID, P>(&entry);
        let (entry, done) = P::receive(&mut context, entry, chunk, step.reborrow(), driver);
        let Some(entry) = entry else { return ControlFlow::Break(()) };
        let entry = match flow::settle::<ID, P>(&mut context, entry, done, step, driver) {
            ControlFlow::Break(_) => return ControlFlow::Break(()),
            ControlFlow::Continue(entry) => entry,
        };
        let Some(mut entry) = entry else { return ControlFlow::Break(()) };
        entry.finish_tail();
        ControlFlow::Continue(true)
    }
}
