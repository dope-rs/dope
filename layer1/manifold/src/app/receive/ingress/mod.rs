use std::{convert, mem, pin};

use dope_core::driver::{self, receive::Stale, route::{self, Identity}, schedule};
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

pub(crate) type Entry<'a, 'p, 'w, 'd, ID, P> = pool::Entry<
    'a, 'p, 'w, 'd, ID,
    <P as Policy<'p, 'w, 'd, ID>>::Transport,
    <P as Policy<'p, 'w, 'd, ID>>::Wire,
    <P as Policy<'p, 'w, 'd, ID>>::State,
    <P as Policy<'p, 'w, 'd, ID>>::Input,
    <P as Policy<'p, 'w, 'd, ID>>::Payload,
    IOV_CAP,
    <P as Policy<'p, 'w, 'd, ID>>::Reserve,
>;

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

enum Control {
    Eof,
    Cancelled,
    Empty(Empty),
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
    entry: &Entry<'_, 'p, 'w, 'd, ID, P>,
) {
    if entry.get().terminal.has_send() {
        entry.pending().wake();
    }
}

fn finish_eof<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID> + 'ctx>(
    context: &mut P::Context<'ctx>,
    entry: Entry<'a, 'p, 'w, 'd, ID, P>,
    end: End,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Entry<'a, 'p, 'w, 'd, ID, P>> {
    if entry.get().is_discarded() {
        P::close(context, entry, CloseCause::Transport, turn, driver)
    } else {
        let entry = P::eof(context, entry, end, turn, driver)?;
        prepare_terminal::<ID, P>(&entry);
        Some(entry)
    }
}

fn discard_with_cause<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID> + 'ctx>(
    context: &mut P::Context<'ctx>,
    entry: Entry<'a, 'p, 'w, 'd, ID, P>,
    err: CloseCause,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Entry<'a, 'p, 'w, 'd, ID, P>> {
    prepare_terminal::<ID, P>(&entry);
    let (entry, terminal) = P::terminal(context, entry, turn.reborrow(), driver);
    let mut entry = entry?;
    match terminal {
        Terminal::Closed | Terminal::Pending => Some(entry),
        Terminal::Ended(_) | Terminal::Idle => {
            entry.get_mut().discard();
            P::close(context, entry, err, turn, driver)
        }
    }
}

fn finish_chunk<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P: Policy<'p, 'w, 'd, ID> + 'ctx>(
    context: &mut P::Context<'ctx>,
    entry: Entry<'a, 'p, 'w, 'd, ID, P>,
    done: Outcome,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Entry<'a, 'p, 'w, 'd, ID, P>> {
    match done {
        done @ (Outcome::Continue | Outcome::CloseAfter) => {
            let (entry, terminal) = P::terminal(context, entry, turn.reborrow(), driver);
            let mut entry = entry?;
            match terminal {
                Terminal::Ended(end) => finish_eof::<ID, P>(context, entry, end, turn, driver),
                Terminal::Closed => Some(entry),
                Terminal::Idle if matches!(done, Outcome::CloseAfter) => {
                    entry.get_mut().close_after();
                    P::finish(context, entry, Finish::CloseAfter, turn, driver)
                }
                Terminal::Pending | Terminal::Idle => {
                    P::finish(context, entry, Finish::Chunk, turn, driver)
                }
            }
        }
        Outcome::Failed(err) => {
            discard_with_cause::<ID, P>(context, entry, CloseCause::Application(err), turn, driver)
        }
    }
}

fn drive_control<'p, 'a, 'c, 'w, 'd, ID: Identity + 'd, P, A, K: route::Selection>(
    policy: pin::Pin<&mut P>,
    ctrl: A,
    turn: schedule::Turn<'a, 'd>,
    driver: &'a mut driver::Context<'c, 'd>,
) where
    'd: 'w + 'p,
    P: Policy<'p, 'w, 'd, ID>,
    A: arrival::Arrival<'d, Observed = event::Control<'d, K>>,
{
    let (pool, mut context) = P::parts(policy);
    let (entry, next) = pool.recv().dispatch(ctrl);
    let next = match next {
        event::Dispatch::Settled => return,
        event::Dispatch::Ready(next) => next,
        event::Dispatch::Observed { completion: done, next } => {
            driver.reclaim(done);
            next
        }
        event::Dispatch::Retired(done)
        | event::Dispatch::Stale { completion: done, cause: Stale } => {
            driver.reclaim(done);
            return;
        }
        event::Dispatch::Rejected { completion: done, error, .. } => {
            driver.reclaim(done);
            if let Some(entry) = entry {
                drop(discard_with_cause::<ID, P>(
                    &mut context, entry, CloseCause::Receive(error), turn, driver,
                ));
            }
            return;
        }
        event::Dispatch::Parked(parked) => {
            if let Some(entry) = entry {
                drop(apply_parked::<ID, P>(&mut context, entry, parked, turn, driver));
            }
            return;
        }
    };
    if let Some(entry) = entry {
        drop(apply_control::<ID, P>(&mut context, entry, next, turn, driver));
    }
}

fn discard_pending<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    context: &mut P::Context<'ctx>,
    entry: Entry<'a, 'p, 'w, 'd, ID, P>,
    mut pending: Chunk<'w, 'd, P::Wire, Disabled>,
    cause: event::ParkError,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Entry<'a, 'p, 'w, 'd, ID, P>>
where
    P: Policy<'p, 'w, 'd, ID> + 'ctx,
{
    pending.drive((), &mut access::Discard);
    drop(pending);
    discard_with_cause::<ID, P>(context, entry, CloseCause::Receive(cause), turn, driver)
}

fn apply_control<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    context: &mut P::Context<'ctx>,
    entry: Entry<'a, 'p, 'w, 'd, ID, P>,
    next: event::ControlRecv<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Entry<'a, 'p, 'w, 'd, ID, P>>
where
    P: Policy<'p, 'w, 'd, ID> + 'ctx,
{
    let end = match next {
        event::ControlRecv::Eof(_) => Control::Eof,
        event::ControlRecv::Cancelled(_) => Control::Cancelled,
        event::ControlRecv::Failed { error, .. } => {
            return P::close(context, entry, CloseCause::Io(error), turn, driver);
        }
        event::ControlRecv::Credit { cause, .. } => {
            return P::close(
                context, entry, CloseCause::Receive(event::ParkError::Credit(cause)), turn, driver,
            );
        }
        event::ControlRecv::NoChunk(_) => Control::Empty(Empty::NoChunk),
        event::ControlRecv::Discarded { finished, .. } => {
            Control::Empty(Empty::Discarded { finished })
        }
    };
    prepare_terminal::<ID, P>(&entry);
    let (entry, terminal) = P::terminal(context, entry, turn.reborrow(), driver);
    let entry = entry?;
    match terminal {
        Terminal::Ended(end) => finish_eof::<ID, P>(context, entry, end, turn, driver),
        Terminal::Closed | Terminal::Pending => Some(entry),
        Terminal::Idle => match end {
            Control::Eof => {
                let slot = entry.get();
                if !slot.is_discarded() && !slot.terminal.is_done() {
                    P::eof(context, entry, End::Read, turn, driver)
                } else {
                    P::close(context, entry, CloseCause::Transport, turn, driver)
                }
            }
            Control::Cancelled => P::close(context, entry, CloseCause::Local, turn, driver),
            Control::Empty(empty) => P::finish(context, entry, Finish::Empty(empty), turn, driver),
        },
    }
}

fn apply_parked<'ctx, 'a, 'p, 'w, 'd: 'w + 'p, ID: Identity + 'd, P>(
    context: &mut P::Context<'ctx>,
    entry: Entry<'a, 'p, 'w, 'd, ID, P>,
    parked: event::ParkRecv<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Entry<'a, 'p, 'w, 'd, ID, P>>
where
    P: Policy<'p, 'w, 'd, ID> + 'ctx,
{
    match parked {
        event::ParkRecv::Closing(_) => P::close(context, entry, CloseCause::Transport, turn, driver),
        event::ParkRecv::Failed { error, .. } => {
            discard_with_cause::<ID, P>(context, entry, CloseCause::Receive(error), turn, driver)
        }
        event::ParkRecv::Deferred => Some(entry),
    }
}
