use std::{convert, mem, pin};

use dope_core::driver::{receive::Stale, retained, route::Identity, schedule};
use dope_net::{
    link::{
        event,
        pool::{self, recv::arrival},
    },
    wire::receive::{
        Chunk,
        mode::{Disabled, access},
    },
};

use crate::app::receive::{End, Failure, Outcome};
pub(crate) mod batch;
mod batched;
mod dispatch;
mod policy;
mod retain;
mod scope;
mod single;
mod strategy;

pub(crate) use batched::Batched;
pub(crate) use dispatch::Dispatch;
pub(crate) use policy::Policy;
pub(crate) use retain::Retain;
pub(crate) use scope::Scope;
pub(in crate::app::receive) use single::Single;
pub(crate) use strategy::{Admission, Replay, Strategy};

pub(crate) const IOV_CAP: usize = 32;

#[derive(Clone, Copy)]
pub(crate) enum Empty {
    /// The wire emitted no application chunk and did not report a discard.
    NoChunk,
    /// The wire consumed input without emitting an application chunk.
    Discarded { finished: bool },
}

#[derive(Clone, Copy)]
pub(crate) enum Finish {
    /// No application chunk was delivered; the wire distinction is preserved.
    Empty(Empty),
    /// The application accepted a delivered chunk or batch.
    Chunk,
    /// The application requested closure after pending egress drains.
    CloseAfter,
}

pub(crate) enum CloseCause {
    /// The local driver cancelled the receive.
    Local,
    /// Transport input failed, starved, or ended during a data transition.
    Transport,
    /// The receive syscall failed with its exact platform error.
    Io(dope_core::Errno),
    /// Application ingress rejected the transition with an exact failure kind.
    Application(Failure),
    /// A deferred receive transition failed at its exact storage or credit leaf.
    Receive(event::ParkError),
}

enum ControlEnd {
    Eof,
    Cancelled,
}

pub(crate) enum Progress<P> {
    Done(Outcome),
    Pending(P),
}

pub(crate) enum Terminal {
    Idle,
    Pending,
    Ended(End),
    Closed,
}

const _: () = assert!(mem::size_of::<Progress<convert::Infallible>>() == mem::size_of::<Outcome>());

fn prepare_terminal<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID>>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
) {
    let send = P::storage(policy.as_mut())
        .get_mut(key)
        .is_some_and(|slot| slot.terminal.has_send());
    if send {
        P::latch(policy, key);
    }
}

fn finish_eof<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID>>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
    end: End,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) {
    let dead = P::storage(policy.as_mut())
        .get_mut(key)
        .is_some_and(|slot| slot.is_discarded());
    if dead {
        P::close(policy, key, CloseCause::Transport, turn, driver);
    } else {
        P::eof(policy.as_mut(), key, end, turn, driver);
        prepare_terminal(policy, key);
    }
}

fn discard_transport<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID>>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
) {
    if let Some(slot) = P::storage(policy.as_mut()).get_mut(key) {
        slot.discard();
    }
}

fn discard_with_cause<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID>>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
    err: CloseCause,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) {
    prepare_terminal(policy.as_mut(), key);
    match P::terminal(policy.as_mut(), key, turn.reborrow(), driver) {
        Terminal::Closed | Terminal::Pending => {}
        Terminal::Ended(_) | Terminal::Idle => {
            discard_transport(policy.as_mut(), key);
            P::close(policy, key, err, turn, driver);
        }
    }
}

fn finish_chunk<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID>>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
    done: Outcome,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) {
    match done {
        Outcome::Continue => match P::terminal(policy.as_mut(), key, turn.reborrow(), driver) {
            Terminal::Ended(end) => {
                finish_eof(policy, key, end, turn, driver);
            }
            Terminal::Closed => {}
            Terminal::Pending | Terminal::Idle => {
                P::finish(policy, key, Finish::Chunk, turn, driver);
            }
        },
        Outcome::CloseAfter => match P::terminal(policy.as_mut(), key, turn.reborrow(), driver) {
            Terminal::Ended(end) => {
                finish_eof(policy, key, end, turn, driver);
            }
            Terminal::Closed => {}
            Terminal::Pending => {
                P::finish(policy, key, Finish::Chunk, turn, driver);
            }
            Terminal::Idle => {
                P::storage(policy.as_mut()).recv().close_after(key);
                P::finish(policy, key, Finish::CloseAfter, turn, driver);
            }
        },
        Outcome::Failed(err) => {
            discard_with_cause(policy, key, CloseCause::Application(err), turn, driver);
        }
    }
}

fn drive_control<'p, 'a, 'c, 'owner, 'w, 'd, ID: Identity + 'd, P, A>(
    mut policy: pin::Pin<&mut P>,
    ctrl: A,
    turn: schedule::Turn<'a, 'd>,
    driver: &'a mut retained::Context<'c, 'owner, 'd>,
) where
    'd: 'owner + 'w + 'p,
    P: Policy<'p, 'w, 'd, ID>,
    A: arrival::Arrival<'d, Observed = event::Control<'d>>,
{
    let next = P::storage(policy.as_mut()).recv().dispatch(ctrl);
    let next = match next {
        event::Dispatch::Settled => return,
        event::Dispatch::Ready(next) => next,
        event::Dispatch::Observed {
            completion: done,
            next,
        } => {
            driver.driver().reclaim(done);
            next
        }
        event::Dispatch::Retired(done) => {
            driver.driver().reclaim(done);
            return;
        }
        event::Dispatch::Rejected {
            completion: done,
            key,
            error,
        } => {
            driver.driver().reclaim(done);
            discard_with_cause(policy, key, CloseCause::Receive(error), turn, driver);
            return;
        }
        event::Dispatch::Parked(parked) => {
            apply_parked(policy, parked, turn, driver);
            return;
        }
        event::Dispatch::Stale {
            completion: done,
            cause: Stale,
        } => {
            driver.driver().reclaim(done);
            return;
        }
    };
    apply_control(policy, next, turn, driver);
}

fn discard_pending<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
    mut pending: Chunk<'w, 'd, P::Wire, Disabled>,
    cause: event::ParkError,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    P: Policy<'p, 'w, 'd, ID>,
{
    pending.drive((), &mut access::Discard);
    drop(pending);
    discard_with_cause(policy, key, CloseCause::Receive(cause), turn, driver);
}

fn apply_control<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    policy: pin::Pin<&mut P>,
    next: event::ControlRecv<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    P: Policy<'p, 'w, 'd, ID>,
{
    match next {
        event::ControlRecv::Eof(key) => {
            finish_control(policy, key, ControlEnd::Eof, turn, driver);
        }
        event::ControlRecv::Cancelled(key) => {
            finish_control(policy, key, ControlEnd::Cancelled, turn, driver);
        }
        event::ControlRecv::Failed { key, error } => {
            P::close(policy, key, CloseCause::Io(error), turn, driver);
        }
        event::ControlRecv::Credit { key, cause } => {
            P::close(
                policy,
                key,
                CloseCause::Receive(event::ParkError::Credit(cause)),
                turn,
                driver,
            );
        }
        event::ControlRecv::NoChunk(key) => {
            finish_empty(policy, key, Empty::NoChunk, turn, driver);
        }
        event::ControlRecv::Discarded { key, finished } => {
            finish_empty(policy, key, Empty::Discarded { finished }, turn, driver);
        }
    }
}

fn finish_control<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
    end: ControlEnd,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    P: Policy<'p, 'w, 'd, ID>,
{
    prepare_terminal(policy.as_mut(), key);
    match P::terminal(policy.as_mut(), key, turn.reborrow(), driver) {
        Terminal::Ended(end) => {
            finish_eof(policy, key, end, turn.reborrow(), driver);
        }
        Terminal::Closed | Terminal::Pending => {}
        Terminal::Idle => match end {
            ControlEnd::Eof => {
                let notify = P::storage(policy.as_mut())
                    .get_mut(key)
                    .is_some_and(|slot| !slot.is_discarded() && !slot.terminal.is_done());
                if notify {
                    P::eof(policy, key, End::Read, turn.reborrow(), driver);
                } else {
                    P::close(policy, key, CloseCause::Transport, turn.reborrow(), driver);
                }
            }
            ControlEnd::Cancelled => {
                P::close(policy, key, CloseCause::Local, turn.reborrow(), driver);
            }
        },
    }
}

fn finish_empty<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    mut policy: pin::Pin<&mut P>,
    key: pool::Key<'d, ID>,
    empty: Empty,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    P: Policy<'p, 'w, 'd, ID>,
{
    prepare_terminal(policy.as_mut(), key);
    match P::terminal(policy.as_mut(), key, turn.reborrow(), driver) {
        Terminal::Ended(end) => {
            finish_eof(policy, key, end, turn.reborrow(), driver);
        }
        Terminal::Closed | Terminal::Pending => {}
        Terminal::Idle => {
            P::finish(policy, key, Finish::Empty(empty), turn.reborrow(), driver);
        }
    }
}

fn apply_parked<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    policy: pin::Pin<&mut P>,
    parked: event::ParkRecv<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    P: Policy<'p, 'w, 'd, ID>,
{
    match parked {
        event::ParkRecv::Closing(key) => {
            P::close(policy, key, CloseCause::Transport, turn, driver);
        }
        event::ParkRecv::Failed { key, error } => {
            discard_with_cause(policy, key, CloseCause::Receive(error), turn, driver);
        }
        event::ParkRecv::Deferred => {}
    }
}
