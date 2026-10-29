use std::{cell, mem};

use dope_core::driver::{route::KeyTag, schedule::ready};

use crate::{
    app::dispatch::identity,
    client::{close, conn, port::error},
};

pub(super) struct Entry<'d, I: identity::Identity> {
    state: cell::Cell<State<'d, I>>,
    write: cell::Cell<WriteState>,
    generation: cell::Cell<Generation>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
struct Generation(u64);

#[derive(Clone, Copy)]
#[repr(u8)]
enum Availability {
    Reusable,
    Retired,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Suspension {
    Suspended,
    Stale,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Signal {
    Shutdown,
    Clear,
}

#[derive(Clone, Copy)]
enum WriteState {
    Open,
    Sealed,
    Close(close::Reason),
    Shutdown,
    CloseAndShutdown(close::Reason),
}

impl WriteState {
    const fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }

    fn close(self, reason: close::Reason) -> Self {
        match self {
            Self::Open | Self::Sealed => Self::Close(reason),
            Self::Shutdown => Self::CloseAndShutdown(reason),
            Self::Close(reason) => Self::Close(reason),
            Self::CloseAndShutdown(reason) => Self::CloseAndShutdown(reason),
        }
    }

    const fn shutdown(self) -> Self {
        match self {
            Self::Open | Self::Sealed | Self::Shutdown => Self::Shutdown,
            Self::Close(reason) | Self::CloseAndShutdown(reason) => Self::CloseAndShutdown(reason),
        }
    }

    const fn take_close(self) -> (Self, Option<close::Reason>) {
        match self {
            Self::Close(reason) => (Self::Sealed, Some(reason)),
            Self::CloseAndShutdown(reason) => (Self::Shutdown, Some(reason)),
            Self::Open | Self::Sealed | Self::Shutdown => (self, None),
        }
    }

    const fn take_signal(self) -> (Self, Signal) {
        match self {
            Self::Shutdown => (Self::Sealed, Signal::Shutdown),
            Self::Open | Self::Sealed | Self::Close(_) | Self::CloseAndShutdown(_) => {
                (self, Signal::Clear)
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Publication<'d, I: identity::Identity> {
    connection: I,
    ready: ready::Target<'d>,
}

impl<'d, I: identity::Identity> Publication<'d, I> {
    fn activate(self, entry: &Entry<'d, I>) {
        entry.write.set(WriteState::Open);
        entry.state.set(State::Active(self));
    }
}

#[derive(Clone, Copy)]
enum State<'d, I: identity::Identity> {
    Idle(Availability),
    Active(Publication<'d, I>),
    Suspended(Publication<'d, I>),
    Publishing(I),
    Draining { connection: I, next: Availability },
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct Transaction<I: identity::Identity> {
    connection: I,
    generation: Generation,
}

pub(super) struct Busy;

#[must_use = "a draining port lane must be cleared before it becomes reusable"]
pub(super) struct Drain<'a, 'd, I: identity::Identity> {
    entry: &'a Entry<'d, I>,
    next: Availability,
}

impl<I: identity::Identity> Drain<'_, '_, I> {
    pub(super) fn finish(self) {
        self.entry.state.set(State::Idle(self.next));
    }
}

impl<'d, I: identity::Identity> Default for Entry<'d, I> {
    fn default() -> Self {
        Self {
            state: cell::Cell::new(State::Idle(Availability::Reusable)),
            write: cell::Cell::new(WriteState::Open),
            generation: cell::Cell::new(Generation::INITIAL),
        }
    }
}

impl<'d, I: identity::Identity> Entry<'d, I> {
    pub(super) fn transaction(&self, conn: I) -> Option<Transaction<I>> {
        let State::Active(live) = self.state.get() else {
            return None;
        };
        (live.connection == conn).then(|| Transaction {
            connection: conn,
            generation: self.generation.get(),
        })
    }

    pub(super) fn writable(&self, conn: I) -> Option<Transaction<I>> {
        self.transaction(conn)
            .filter(|_| self.write.get().is_open())
    }

    pub(super) fn can_write(&self, proof: Transaction<I>) -> bool {
        self.transaction(proof.connection) == Some(proof) && self.write.get().is_open()
    }

    pub(super) fn is_suspended(&self, proof: Transaction<I>) -> bool {
        if self.generation.get() != proof.generation {
            return false;
        }
        matches!(
            self.state.get(),
            State::Suspended(live)
                if live.connection == proof.connection
        )
    }

    pub(super) fn suspend(&self, proof: Transaction<I>) -> Suspension {
        if self.generation.get() != proof.generation {
            return Suspension::Stale;
        }
        let State::Active(live) = self.state.get() else {
            return Suspension::Stale;
        };
        if live.connection != proof.connection {
            return Suspension::Stale;
        }
        self.state.set(State::Suspended(live));
        Suspension::Suspended
    }

    pub(super) fn restore(&self, proof: Transaction<I>) {
        if self.generation.get() != proof.generation {
            return;
        }
        let State::Suspended(live) = self.state.get() else {
            return;
        };
        if live.connection != proof.connection {
            return;
        }
        self.state.set(State::Active(live));
    }

    pub(super) fn try_activate(
        &self,
        conn: I,
        ready: ready::Target<'d>,
    ) -> Result<(), error::Open> {
        match self.state.get() {
            State::Idle(Availability::Reusable) => {}
            State::Idle(Availability::Retired) => return Err(error::Open::Exhausted),
            State::Active(_)
            | State::Suspended(_)
            | State::Publishing(_)
            | State::Draining { .. } => {
                return Err(error::Open::Occupied);
            }
        }
        if matches!(self.advance(), Availability::Retired) {
            self.state.set(State::Idle(Availability::Retired));
            return Err(error::Open::Exhausted);
        }
        Publication {
            connection: conn,
            ready,
        }
        .activate(self);
        Ok(())
    }

    pub(super) fn begin_retire(&self, conn: I) -> Result<Option<Drain<'_, 'd, I>>, Busy> {
        let next = match self.state.get() {
            State::Publishing(owner) if owner == conn => {
                if self.write.get().is_open() {
                    self.write.set(WriteState::Sealed);
                }
                return Err(Busy);
            }
            State::Active(live) | State::Suspended(live) if live.connection == conn => {
                self.write.set(WriteState::Sealed);
                let next = self.advance();
                self.state.set(State::Draining {
                    connection: conn,
                    next,
                });
                next
            }
            State::Draining {
                connection: owner,
                next,
            } if owner == conn => next,
            State::Idle(_)
            | State::Active(_)
            | State::Suspended(_)
            | State::Publishing(_)
            | State::Draining { .. } => {
                return Ok(None);
            }
        };
        Ok(Some(Drain { entry: self, next }))
    }

    /// Seals writers and restores Active, invalidating the suspended loan.
    /// Its exclusive Requests borrow prevents another suspension until that
    /// loan ends, even when the generation cannot advance.
    pub(super) fn close(&self, conn: I, reason: close::Reason) {
        match self.state.get() {
            State::Publishing(owner) if owner == conn => {
                self.write.set(self.write.get().close(reason));
            }
            State::Active(live) if live.connection == conn => {
                self.write.set(self.write.get().close(reason));
                live.ready.wake();
            }
            State::Suspended(live) if live.connection == conn => {
                self.write.set(self.write.get().close(reason));
                self.advance();
                self.state.set(State::Active(live));
                live.ready.wake();
            }
            State::Draining {
                connection: live, ..
            } if live == conn => {
                self.write.set(self.write.get().close(reason));
            }
            State::Idle(_)
            | State::Active(_)
            | State::Suspended(_)
            | State::Publishing(_)
            | State::Draining { .. } => {}
        }
    }

    pub(super) fn take_close(&self, proof: Transaction<I>) -> Option<close::Reason> {
        if self.transaction(proof.connection) != Some(proof) {
            return None;
        }
        let (next, reason) = self.write.get().take_close();
        self.write.set(next);
        reason
    }

    pub(super) fn shutdown(&self, conn: I) -> Result<(), error::Shutdown> {
        let ready = match self.state.get() {
            State::Publishing(owner) if owner == conn => None,
            State::Active(live) if live.connection == conn => Some(live.ready),
            _ => return Err(error::Shutdown::Inactive),
        };
        self.write.set(self.write.get().shutdown());
        if let Some(ready) = ready {
            ready.wake();
        }
        Ok(())
    }

    pub(super) fn take_signal(&self, proof: Transaction<I>) -> Signal {
        if self.transaction(proof.connection) != Some(proof) {
            return Signal::Clear;
        }
        let (next, signal) = self.write.get().take_signal();
        self.write.set(next);
        signal
    }

    pub(super) fn signal(&self) -> Signal {
        match self.write.get() {
            WriteState::Shutdown | WriteState::CloseAndShutdown(_) => Signal::Shutdown,
            WriteState::Open | WriteState::Sealed | WriteState::Close(_) => Signal::Clear,
        }
    }

    /// The validated permit retains the region token until this call. Only
    /// its synchronous callback can lend that token to retirement, so keep
    /// the lane occupied until publication or rollback has completed.
    pub(super) fn publish<R>(&self, proof: Transaction<I>, publish: impl FnOnce() -> R) -> R {
        let previous = self.state.replace(State::Publishing(proof.connection));
        let result = publish();
        self.state.set(previous);
        self.mark_ready(proof);
        result
    }

    pub(super) fn mark_ready(&self, proof: Transaction<I>) {
        if self.generation.get() != proof.generation {
            return;
        }
        if let State::Active(live) = self.state.get()
            && live.connection == proof.connection
        {
            live.ready.wake();
        }
    }

    fn advance(&self) -> Availability {
        let Some(next) = self.generation.get().next() else {
            return Availability::Retired;
        };
        self.generation.set(next);
        Availability::Reusable
    }
}

impl Generation {
    const INITIAL: Self = Self(0);

    const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

const _: () = assert!(mem::size_of::<Availability>() == mem::size_of::<bool>());
const _: () = assert!(mem::size_of::<Suspension>() == mem::size_of::<bool>());
const _: () = assert!(mem::size_of::<Signal>() == mem::size_of::<bool>());
const _: () = assert!(
    mem::size_of::<Transaction<conn::Id<'static, KeyTag<0>>>>()
        == mem::size_of::<conn::Id<'static, KeyTag<0>>>() + mem::size_of::<Generation>()
);

const _: () = assert!(
    mem::size_of::<State<'static, conn::Id<'static, KeyTag<0>>>>()
        <= mem::size_of::<Publication<'static, conn::Id<'static, KeyTag<0>>>>()
            + mem::size_of::<usize>()
);
