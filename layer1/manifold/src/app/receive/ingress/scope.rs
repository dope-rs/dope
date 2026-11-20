use std::{ops::ControlFlow, pin};

use dope_core::driver::{self, route::{self, Identity}, schedule};
use dope_net::{
    link::{
        event,
        pool::recv::{Deferral, arrival, delivery, input, reserve::Choice},
    },
    wire,
    wire::receive::{Chunk, mode::Disabled},
};

use crate::app::receive::{Outcome, ingress::{self, Admission as _, Dispatch, Replay, Strategy}};

pub(crate) struct Scope;

impl<W, M> Strategy<W, M> for Scope
where
    W: wire::Spec,
    M: Dispatch<W, Strategy = Scope>,
    M::Kind: delivery::ScopedData + input::Replay<W>,
{
    type Value<'input, 'w, 'd>
        = &'input mut Chunk<'w, 'd, W, Disabled>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;

    type Completion = Option<Outcome>;

    type Admission<'a, 'c, 'd>
        = ingress::Single<'a, 'c, 'd>
    where
        'c: 'a,
        'd: 'a + 'c,
        W: 'd;

    fn drive_data<'p, 'a, 'c, 'w, 'd, ID: Identity + 'd, P, A, K: route::Selection>(
        policy: pin::Pin<&mut P>,
        data: A,
        work: Self::Admission<'a, 'c, 'd>,
    ) where
        'c: 'a,
        'd: 'a + 'c + 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, input::Receipt<W, M>, Observed = event::Data<'d, (), K>>,
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
                    match deliver::<ID, P>(context, entry, chunk, turn, driver) {
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
        let ControlFlow::Continue(Some(mut entry)) =
            deliver::<ID, P>(&mut context, entry, chunk, step, driver)
        else {
            return ControlFlow::Break(());
        };
        entry.finish_tail();
        ControlFlow::Continue(true)
    }
}

fn deliver<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    context: &mut P::Context<'ctx>,
    entry: ingress::Entry<'a, 'p, 'w, 'd, ID, P>,
    mut chunk: Chunk<'w, 'd, P::Wire, Disabled>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> ControlFlow<Option<ingress::Entry<'a, 'p, 'w, 'd, ID, P>>, Option<ingress::Entry<'a, 'p, 'w, 'd, ID, P>>>
where
    P: ingress::Policy<'p, 'w, 'd, ID> + 'ctx,
    P::Input: Dispatch<P::Wire, Strategy = Scope>,
    <P::Input as input::Mode<P::Wire>>::Kind: delivery::ScopedData + input::Replay<P::Wire>,
{
    ingress::prepare_terminal::<ID, P>(&entry);
    let (entry, done) = P::receive(context, entry, &mut chunk, turn.reborrow(), driver);
    let Some(mut entry) = entry else { return ControlFlow::Break(None) };
    if let Some(outcome) = done {
        drop(chunk);
        return ControlFlow::Continue(ingress::finish_chunk::<ID, P>(context, entry, outcome, turn, driver));
    }
    let (pending, cause) = match entry.defer(chunk) {
        Deferral::Ready => {
            return ControlFlow::Break(P::finish(context, entry, ingress::Finish::Chunk, turn, driver));
        }
        Deferral::Unavailable(pending) => (pending, event::ParkError::Unavailable),
        Deferral::Occupied(pending) => (pending, event::ParkError::Occupied),
    };
    ControlFlow::Continue(ingress::discard_pending::<ID, P>(context, entry, pending, cause, turn, driver))
}
