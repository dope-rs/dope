use std::{cell, mem};

use dope_core::driver::schedule::ready;

use crate::{connector::connection, dispatch::typed::identity};

pub(super) struct Entry<'d, I: identity::Identity> {
    state: cell::Cell<State<'d, I>>,
    close: cell::Cell<bool>,
    generation: cell::Cell<u64>,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Availability {
    Reusable,
    Retired,
}

#[derive(Clone, Copy)]
struct Publication<'d, I: identity::Identity> {
    connection: I,
    ready: ready::Target<'d>,
}

#[derive(Clone, Copy)]
enum State<'d, I: identity::Identity> {
    Idle(Availability),
    Active(Publication<'d, I>),
    Suspended(Publication<'d, I>),
    Draining { connection: I, next: Availability },
}

#[derive(Clone, Copy)]
pub(super) struct Transaction<I: identity::Identity> {
    connection: I,
    generation: u64,
}

impl<'d, I: identity::Identity> Entry<'d, I> {
    pub(super) fn new() -> Self {
        Self {
            state: cell::Cell::new(State::Idle(Availability::Reusable)),
            close: cell::Cell::new(false),
            generation: cell::Cell::new(0),
        }
    }

    pub(super) fn transaction(&self, connection: I) -> Option<Transaction<I>> {
        let State::Active(publication) = self.state.get() else {
            return None;
        };
        (publication.connection == connection).then(|| Transaction {
            connection,
            generation: self.generation.get(),
        })
    }

    pub(super) fn is_active(&self, transaction: Transaction<I>) -> bool {
        if self.generation.get() != transaction.generation {
            return false;
        }
        matches!(
            self.state.get(),
            State::Active(publication)
                if publication.connection == transaction.connection
        )
    }

    pub(super) fn is_suspended(&self, transaction: Transaction<I>) -> bool {
        if self.generation.get() != transaction.generation {
            return false;
        }
        matches!(
            self.state.get(),
            State::Suspended(publication)
                if publication.connection == transaction.connection
        )
    }

    pub(super) fn suspend(&self, transaction: Transaction<I>) -> bool {
        if self.generation.get() != transaction.generation {
            return false;
        }
        let State::Active(publication) = self.state.get() else {
            return false;
        };
        if publication.connection != transaction.connection {
            return false;
        }
        self.state.set(State::Suspended(publication));
        true
    }

    pub(super) fn restore(&self, transaction: Transaction<I>) -> bool {
        if self.generation.get() != transaction.generation {
            return false;
        }
        let State::Suspended(publication) = self.state.get() else {
            return false;
        };
        if publication.connection != transaction.connection {
            return false;
        }
        self.state.set(State::Active(publication));
        true
    }

    pub(super) fn activate(
        &self,
        connection: I,
        ready: ready::Target<'d>,
        lane_empty: impl FnOnce() -> bool,
    ) -> bool {
        if !matches!(self.state.get(), State::Idle(Availability::Reusable)) {
            return false;
        }
        let Some(generation) = self.advance_generation() else {
            self.state.set(State::Idle(Availability::Retired));
            return false;
        };
        assert!(
            lane_empty(),
            "connector port lane reused before request retirement"
        );
        let publication = Publication { connection, ready };
        let transaction = Transaction {
            connection,
            generation,
        };
        self.close.set(false);
        self.state.set(State::Active(publication));
        self.is_active(transaction)
    }

    pub(super) fn begin_retirement(&self, connection: I) -> Option<Availability> {
        let next = match self.state.get() {
            State::Active(publication) | State::Suspended(publication)
                if publication.connection == connection =>
            {
                self.close.set(false);
                let next = if self.advance_generation().is_some() {
                    Availability::Reusable
                } else {
                    Availability::Retired
                };
                self.state.set(State::Draining { connection, next });
                next
            }
            State::Draining {
                connection: owner,
                next,
            } if owner == connection => next,
            State::Idle(_) | State::Active(_) | State::Suspended(_) | State::Draining { .. } => {
                return None;
            }
        };
        Some(next)
    }

    pub(super) fn finish_retirement(&self, next: Availability) {
        self.state.set(State::Idle(next));
    }

    pub(super) fn close(&self, connection: I) {
        match self.state.get() {
            State::Active(publication) if publication.connection == connection => {
                self.close.set(true);
                publication.ready.wake();
            }
            State::Suspended(publication) if publication.connection == connection => {
                if self.advance_generation().is_some() {
                    self.state.set(State::Active(publication));
                    self.close.set(true);
                    publication.ready.wake();
                    return;
                }
                self.close.set(false);
                self.state.set(State::Draining {
                    connection,
                    next: Availability::Retired,
                });
                publication.ready.wake();
            }
            State::Idle(_) | State::Active(_) | State::Suspended(_) | State::Draining { .. } => {}
        }
    }

    pub(super) fn take_close(&self, transaction: Transaction<I>) -> bool {
        self.is_active(transaction) && self.close.take()
    }

    pub(super) fn mark_ready(&self, transaction: Transaction<I>) {
        if self.generation.get() != transaction.generation {
            return;
        }
        if let State::Active(publication) = self.state.get()
            && publication.connection == transaction.connection
        {
            publication.ready.wake();
        }
    }

    fn advance_generation(&self) -> Option<u64> {
        let next = self.generation.get().checked_add(1)?;
        self.generation.set(next);
        Some(next)
    }
}

const _: () = assert!(mem::size_of::<Availability>() == mem::size_of::<bool>());
const _: () = assert!(
    mem::size_of::<Transaction<connection::Id<'static, 0>>>()
        == mem::size_of::<connection::Id<'static, 0>>() + mem::size_of::<u64>()
);
