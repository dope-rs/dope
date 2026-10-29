use std::{convert, ops::ControlFlow, pin};

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::{
        event,
        pool::{
            self,
            recv::{Deferral, input},
        },
    },
    wire::receive::{Chunk, mode::Disabled},
};

use crate::app::receive::ingress::{self, Policy, Progress};

/// Dispatches one chunk; inline settlement leaves no replay tail to skip.
pub(in crate::app::receive::ingress) fn apply<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P, C, F>(
    mut policy: pin::Pin<&mut P>,
    next: event::DispatchRecv<'d, ID, C>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
    read: F,
) where
    P: Policy<'p, 'w, 'd, ID>,
    <P::Input as input::Mode<P::Wire>>::Kind: input::Replay<P::Wire>,
    F: FnOnce(
        pin::Pin<&mut P>,
        pool::Key<'d, ID>,
        C,
        schedule::Turn<'_, 'd>,
        &mut retained::Context<'_, '_, 'd>,
    ) -> Progress<Chunk<'w, 'd, P::Wire, Disabled>>,
{
    match next {
        event::DispatchRecv::Control(next) => {
            ingress::apply_control(policy, next, turn, driver);
        }
        event::DispatchRecv::Chunk(key, chunk) => {
            ingress::prepare_terminal(policy.as_mut(), key);
            let done = read(policy.as_mut(), key, chunk, turn.reborrow(), driver);
            let _ = settle(policy, key, done, turn, driver);
        }
    }
}

pub(in crate::app::receive::ingress) fn settle<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
    done: Progress<Chunk<'w, 'd, P::Wire, Disabled>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) -> ControlFlow<()>
where
    P: Policy<'p, 'w, 'd, ID>,
    <P::Input as input::Mode<P::Wire>>::Kind: input::Replay<P::Wire>,
{
    match done {
        Progress::Pending(pending) => {
            let deferred = P::storage(policy.as_mut()).recv().defer(key, pending);
            let (pending, cause) = match deferred {
                Deferral::Ready => {
                    P::finish(policy, key, ingress::Finish::Chunk, turn, driver);
                    return ControlFlow::Break(());
                }
                Deferral::Unavailable(pending) => (pending, event::ParkError::Unavailable),
                Deferral::Occupied(pending) => (pending, event::ParkError::Occupied),
                Deferral::Full(pending) => (pending, event::ParkError::Full),
            };
            ingress::discard_pending(policy, key, pending, cause, turn, driver);
        }
        Progress::Done(done) => ingress::finish_chunk(policy, key, done, turn, driver),
    }
    ControlFlow::Continue(())
}

pub(in crate::app::receive::ingress) fn apply_done<
    'p,
    'w,
    'd: 'w + 'p,
    ID: Identity + 'd,
    P,
    C,
    F,
>(
    mut policy: pin::Pin<&mut P>,
    next: event::DispatchRecv<'d, ID, C>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
    read: F,
) where
    P: Policy<'p, 'w, 'd, ID>,
    F: FnOnce(
        pin::Pin<&mut P>,
        pool::Key<'d, ID>,
        C,
        schedule::Turn<'_, 'd>,
        &mut retained::Context<'_, '_, 'd>,
    ) -> Progress<convert::Infallible>,
{
    match next {
        event::DispatchRecv::Control(next) => {
            ingress::apply_control(policy, next, turn, driver);
        }
        event::DispatchRecv::Chunk(key, chunk) => {
            ingress::prepare_terminal(policy.as_mut(), key);
            match read(policy.as_mut(), key, chunk, turn.reborrow(), driver) {
                Progress::Pending(pending) => match pending {},
                Progress::Done(done) => {
                    ingress::finish_chunk(policy, key, done, turn.reborrow(), driver);
                }
            }
        }
    }
}
