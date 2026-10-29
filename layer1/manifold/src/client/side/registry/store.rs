use std::{cell::UnsafeCell, mem, task};

use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::ready::wake,
};
use dope_net::link::pool;
use o3::{
    alloc,
    depot::{array, capacity, queue::indexed::lanes},
};

use crate::client::{conn, side, side::registry};

type Table<'d, I, P, ID> = lanes::Table<pool::raw::Key<'d, ID>, State<'d, I, P>, 3>;

type Rejection<'d, I, P> = Option<(Source<I, P>, Option<wake::Waker<'d>>)>;

const QUEUED: array::Index<3> = array::Index::at::<0>();
const ABANDONED: array::Index<3> = array::Index::at::<1>();
const DEFERRED: array::Index<3> = array::Index::at::<2>();

pub(super) enum Source<I, P> {
    Intent(I),
    Payload(P),
}

struct Pending<'d, T> {
    value: T,
    waiter: Option<wake::Waker<'d>>,
}

enum State<'d, I, P> {
    Queued(Pending<'d, I>),
    InFlight { waiter: Option<wake::Waker<'d>> },
    Deferred(Pending<'d, P>),
    Settled { result: Result<(), side::Error> },
    Abandoned,
}

impl<'d, I, P> State<'d, I, P> {
    fn take(&mut self) -> Option<Source<I, P>> {
        let (source, waiter) = match mem::replace(self, Self::InFlight { waiter: None }) {
            Self::Queued(Pending { value, waiter }) => (Source::Intent(value), waiter),
            Self::Deferred(Pending { value, waiter }) => (Source::Payload(value), waiter),
            state => {
                *self = state;
                return None;
            }
        };
        *self = Self::InFlight { waiter };
        Some(source)
    }

    fn reject(&mut self) -> Rejection<'d, I, P> {
        match mem::replace(
            self,
            Self::Settled {
                result: Err(side::Error::IntakeClosed),
            },
        ) {
            Self::Queued(Pending { value, waiter }) => Some((Source::Intent(value), waiter)),
            Self::Deferred(Pending { value, waiter }) => Some((Source::Payload(value), waiter)),
            state => {
                *self = state;
                None
            }
        }
    }
}

/// Thread-local lane owner. Each method completes its exclusive table borrow
/// before returning or invoking code outside this module.
#[repr(transparent)]
pub(in crate::client::side) struct Store<'d, I, P, ID: Identity>(UnsafeCell<Table<'d, I, P, ID>>);

const _: () = assert!(
    mem::size_of::<Store<'static, (), (), KeyTag<0>>>()
        == mem::size_of::<Table<'static, (), (), KeyTag<0>>>()
);

impl<'d, I, P, ID: Identity> Store<'d, I, P, ID> {
    pub(super) fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self(UnsafeCell::new(lanes::Table::try_new(capacity)?)))
    }

    pub(super) fn try_reserve(
        &self,
        intent: I,
        waiter: wake::Waker<'d>,
    ) -> Result<(), registry::Rejected<'d, I, ID>>
    where
        I: registry::Intent<'d, ID>,
    {
        let target = intent.target();
        self.table(|table| {
            let vacant = match table.try_vacant(Self::key(target)) {
                Ok(vacant) => vacant,
                Err(cause) => {
                    return Err(registry::Rejected {
                        intent,
                        waiter,
                        cause: registry::Cause::Slot(cause),
                    });
                }
            };
            vacant
                .insert(State::Queued(Pending {
                    value: intent,
                    waiter: Some(waiter),
                }))
                .move_to_back(QUEUED);
            Ok(())
        })
    }

    pub(super) fn poll(
        &self,
        target: conn::Id<'d, ID>,
        waiter: wake::Waker<'d>,
    ) -> task::Poll<Result<(), side::Error>> {
        self.table(|table| {
            let Some(mut entry) = table.entry(Self::key(target)) else {
                return task::Poll::Ready(Err(side::Error::Stale));
            };
            let result = match entry.get_mut() {
                State::Queued(Pending { waiter: slot, .. })
                | State::InFlight { waiter: slot }
                | State::Deferred(Pending { waiter: slot, .. }) => {
                    *slot = Some(waiter);
                    return task::Poll::Pending;
                }
                State::Settled { result } => Some(*result),
                State::Abandoned => None,
            };
            let Some(result) = result else {
                return task::Poll::Ready(Err(side::Error::Stale));
            };
            entry.remove();
            task::Poll::Ready(result)
        })
    }

    /// The removed state is destroyed by this statement, after the table loan
    /// ends, so no user destructor runs while the table is borrowed.
    pub(super) fn abandon(&self, target: conn::Id<'d, ID>) {
        self.table(|table| {
            let mut entry = table.entry(Self::key(target))?;
            match entry.get_mut() {
                State::Queued(_) | State::Deferred(_) | State::Settled { .. } => {
                    return Some(entry.remove());
                }
                State::InFlight { .. } => {
                    *entry.get_mut() = State::Abandoned;
                    entry.move_to_back(ABANDONED);
                }
                State::Abandoned => {}
            }
            None
        });
    }

    pub(super) fn pending(&self) -> registry::Pending {
        self.table(|table| registry::Pending {
            ready: !table.is_vacant(QUEUED),
            deferred: !table.is_vacant(DEFERRED),
            cancel: !table.is_vacant(ABANDONED),
        })
    }

    pub(super) fn retry(&self) {
        self.table(|table| table.move_all(DEFERRED, QUEUED));
    }

    pub(super) fn take_request(&self) -> Option<(side::Ticket<'d, ID>, Source<I, P>)> {
        self.table(|table| {
            let mut front = table.front(QUEUED)?;
            let key = front.key();
            let source = front.get_mut().take()?;
            front.unlink();
            let target = conn::Id::from_key(key.into_key());
            Some((side::Ticket { target }, source))
        })
    }

    /// The payload moves either into the deferred state or into the returned
    /// option, which this statement destroys after the table loan ends.
    /// Only an abandoned state, containing no user owner, is removed inside the loan.
    pub(super) fn defer(&self, ticket: side::Ticket<'d, ID>, payload: P) {
        drop(self.table(move |table| {
            let target = ticket.target();
            let Some(mut entry) = table.entry(Self::key(target)) else {
                return Some(payload);
            };
            match entry.get_mut() {
                State::InFlight { waiter } => {
                    let waiter = waiter.take();
                    *entry.get_mut() = State::Deferred(Pending {
                        value: payload,
                        waiter,
                    });
                    entry.move_to_back(DEFERRED);
                    None
                }
                State::Abandoned => {
                    entry.remove();
                    Some(payload)
                }
                State::Queued(_) | State::Deferred(_) | State::Settled { .. } => Some(payload),
            }
        }));
    }

    pub(super) fn complete(
        &self,
        ticket: side::Ticket<'d, ID>,
        result: Result<(), side::Error>,
    ) -> Option<wake::Waker<'d>> {
        self.table(|table| {
            let target = ticket.target();
            let mut entry = table.entry(Self::key(target))?;
            match entry.get_mut() {
                State::InFlight { waiter } => {
                    let waiter = waiter.take();
                    *entry.get_mut() = State::Settled { result };
                    waiter
                }
                State::Abandoned => {
                    entry.remove();
                    None
                }
                State::Queued(_) | State::Deferred(_) | State::Settled { .. } => None,
            }
        })
    }

    pub(super) fn take_cancel(&self) -> Option<side::Cancellation<'d, ID>> {
        self.table(|table| {
            let front = table.front(ABANDONED)?;
            let target = conn::Id::from_key(front.key().into_key());
            front.unlink();
            Some(side::Cancellation { target })
        })
    }

    pub(super) fn end_cancel(
        &self,
        cancel: side::Cancellation<'d, ID>,
        result: Result<(), side::Error>,
    ) {
        self.table(|table| {
            let target = cancel.target();
            let Some(mut entry) = table.entry(Self::key(target)) else {
                return;
            };
            if matches!(entry.get_mut(), State::Abandoned) {
                match result {
                    Ok(()) => {
                        entry.remove();
                    }
                    Err(error) => *entry.get_mut() = State::Settled { result: Err(error) },
                }
            }
        })
    }

    pub(super) fn reject(&self) -> Option<Option<wake::Waker<'d>>> {
        let result = self.table(|table| {
            let lane = if table.is_vacant(QUEUED) {
                DEFERRED
            } else {
                QUEUED
            };
            let mut front = table.front(lane)?;
            let result = front.get_mut().reject()?;
            front.unlink();
            Some(result)
        });
        result.map(|(_, waiter)| waiter)
    }

    fn key(target: conn::Id<'d, ID>) -> pool::raw::Key<'d, ID> {
        pool::raw::Key::new(target.key())
    }

    fn table<R>(&self, run: impl for<'a> FnOnce(&'a mut Table<'d, I, P, ID>) -> R) -> R {
        // SAFETY: Store is !Sync and these private operations neither reenter
        // nor run user code. Removed user owners leave this loan before Drop.
        unsafe { run(&mut *self.0.get()) }
    }
}
