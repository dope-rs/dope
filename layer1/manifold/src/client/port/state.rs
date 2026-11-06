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
    Close(close::Directive),
    Shutdown,
}

impl WriteState {
    const fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }

    const fn shutdown(self) -> Self {
        match self {
            Self::Open | Self::Sealed | Self::Shutdown => Self::Shutdown,
            Self::Close(_) => self,
        }
    }
}

#[derive(Clone, Copy)]
struct Publication<'d, I: identity::Identity> {
    connection: I,
    ready: ready::Target<'d>,
}

#[derive(Clone, Copy)]
enum State<'d, I: identity::Identity> {
    Idle,
    Active(Publication<'d, I>),
    Suspended(Publication<'d, I>),
    Publishing(I),
    Draining(Publication<'d, I>),
    Retired(Publication<'d, I>),
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
}

impl<I: identity::Identity> Drain<'_, '_, I> {
    pub(super) fn finish(self) {
        let State::Draining(live) = self.entry.state.get() else {
            unreachable!("drain");
        };
        self.entry.state.set(State::Retired(live));
    }
}

impl<'d, I: identity::Identity> Default for Entry<'d, I> {
    fn default() -> Self {
        Self {
            state: cell::Cell::new(State::Idle),
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
        if self.generation.get() != proof.generation
            || matches!(self.write.get(), WriteState::Close(_))
        {
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
            State::Idle => {}
            State::Retired(live) if live.connection != conn => {}
            State::Active(_)
            | State::Suspended(_)
            | State::Publishing(_)
            | State::Draining(_)
            | State::Retired(_) => {
                return Err(error::Open::Occupied);
            }
        }
        self.advance().ok_or(error::Open::Exhausted)?;
        self.write.set(WriteState::Open);
        self.state.set(State::Active(Publication {
            connection: conn,
            ready,
        }));
        Ok(())
    }

    pub(super) fn begin_retire(&self, conn: I) -> Result<Option<Drain<'_, 'd, I>>, Busy> {
        match self.state.get() {
            State::Publishing(owner) if owner == conn => {
                if self.write.get().is_open() {
                    self.write.set(WriteState::Sealed);
                }
                return Err(Busy);
            }
            State::Active(live) | State::Suspended(live) if live.connection == conn => {
                if !matches!(self.write.get(), WriteState::Close(_)) {
                    self.write.set(WriteState::Sealed);
                }
                let _ = self.advance();
                self.state.set(State::Draining(live));
            }
            State::Draining(live) if live.connection == conn => {}
            State::Idle
            | State::Active(_)
            | State::Suspended(_)
            | State::Publishing(_)
            | State::Draining(_)
            | State::Retired(_) => {
                return Ok(None);
            }
        }
        Ok(Some(Drain { entry: self }))
    }

    /// Seals writers and restores Active, invalidating the suspended loan.
    /// Its exclusive Requests borrow prevents another suspension until that
    /// loan ends, even when the generation cannot advance.
    pub(super) fn close(&self, conn: I, rule: close::Directive) {
        let ready = match self.state.get() {
            State::Publishing(owner) if owner == conn => None,
            State::Active(live) | State::Draining(live) | State::Retired(live)
                if live.connection == conn =>
            {
                Some(live.ready)
            }
            State::Suspended(live) if live.connection == conn => {
                let _ = self.advance();
                self.state.set(State::Active(live));
                Some(live.ready)
            }
            State::Idle
            | State::Active(_)
            | State::Suspended(_)
            | State::Publishing(_)
            | State::Draining(_)
            | State::Retired(_) => return,
        };
        let rule = match self.write.get() {
            WriteState::Close(current) => current.merge(rule),
            WriteState::Open | WriteState::Sealed | WriteState::Shutdown => rule,
        };
        self.write.set(WriteState::Close(rule));
        if let Some(ready) = ready {
            ready.wake();
        }
    }

    pub(super) fn take_close(&self, conn: I) -> Option<close::Directive> {
        match self.state.get() {
            State::Active(live)
            | State::Suspended(live)
            | State::Draining(live)
            | State::Retired(live)
                if live.connection == conn => {}
            State::Publishing(owner) if owner == conn => {}
            _ => return None,
        }
        let WriteState::Close(rule) = self.write.get() else {
            return None;
        };
        self.write.set(WriteState::Sealed);
        Some(rule)
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
        let signal = self.signal();
        if matches!(signal, Signal::Shutdown) {
            self.write.set(WriteState::Sealed);
        }
        signal
    }

    pub(super) fn signal(&self) -> Signal {
        match self.write.get() {
            WriteState::Shutdown => Signal::Shutdown,
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

    fn advance(&self) -> Option<()> {
        let next = self.generation.get().next()?;
        self.generation.set(next);
        Some(())
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
