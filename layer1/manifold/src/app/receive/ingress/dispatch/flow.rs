use std::{convert, ops::ControlFlow};

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{
    link::{
        event,
        pool::recv::{Deferral, input},
    },
    wire::receive::{Chunk, mode::Disabled},
};

use crate::app::receive::ingress::{self, Policy, Progress};

pub(in crate::app::receive::ingress) fn settle<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    context: &mut P::Context<'ctx>,
    mut entry: ingress::Entry<'a, 'p, 'w, 'd, ID, P>,
    done: Progress<Chunk<'w, 'd, P::Wire, Disabled>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> ControlFlow<Option<ingress::Entry<'a, 'p, 'w, 'd, ID, P>>, Option<ingress::Entry<'a, 'p, 'w, 'd, ID, P>>>
where
    P: Policy<'p, 'w, 'd, ID> + 'ctx,
    <P::Input as input::Mode<P::Wire>>::Kind: input::Replay<P::Wire>,
{
    let entry = match done {
        Progress::Pending(pending) => {
            let (pending, cause) = match entry.defer(pending) {
                Deferral::Ready => {
                    return ControlFlow::Break(P::finish(
                        context, entry, ingress::Finish::Chunk, turn, driver,
                    ));
                }
                Deferral::Unavailable(pending) => (pending, event::ParkError::Unavailable),
                Deferral::Occupied(pending) => (pending, event::ParkError::Occupied),
            };
            ingress::discard_pending::<ID, P>(context, entry, pending, cause, turn, driver)
        }
        Progress::Done(done) => ingress::finish_chunk::<ID, P>(context, entry, done, turn, driver),
    };
    ControlFlow::Continue(entry)
}

pub(in crate::app::receive::ingress) fn apply_done<
    'ctx,
    'input,
    'a,
    'p,
    'w: 'input,
    'd: 'w + 'p,
    ID: Identity + 'd,
    P,
>(
    context: &mut P::Context<'ctx>,
    entry: ingress::Entry<'a, 'p, 'w, 'd, ID, P>,
    next: event::DispatchRecv<'d, ID, crate::Incoming<'input, 'w, 'd, P::Wire, P::Input>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, P>>
where
    P: Policy<'p, 'w, 'd, ID> + 'ctx,
    crate::Ingress<P::Wire, P::Input>:
        ingress::Strategy<P::Wire, P::Input, Pending<'w, 'd> = convert::Infallible>,
{
    match next {
        event::DispatchRecv::Control(next) => {
            ingress::apply_control::<ID, P>(context, entry, next, turn, driver)
        }
        event::DispatchRecv::Chunk(_, chunk) => {
            ingress::prepare_terminal::<ID, P>(&entry);
            let (entry, progress) = P::receive(context, entry, chunk, turn.reborrow(), driver);
            let entry = entry?;
            match progress {
                Progress::Pending(pending) => match pending {},
                Progress::Done(done) => {
                    ingress::finish_chunk::<ID, P>(context, entry, done, turn, driver)
                }
            }
        }
    }
}
