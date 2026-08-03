use std::cell::Cell;

use dope_core::driver::ready::ReadyKey;
use dope_core::driver::token::Token;
use dope_net::link::egress::StableBytes;
use dope_net::link::egress::config::Config;
use dope_net::link::egress::metadata;
use o3::cell::RegionToken;

use super::app::{CloseKind, Requests};
use crate::DriverRef;

struct Entry<'d> {
    state: Cell<State<'d>>,
    lane: usize,
    close: Cell<bool>,
    generation: Cell<u64>,
}

#[derive(Clone, Copy)]
struct Publication<'d> {
    token: Token,
    ready: ReadyKey<'d>,
}

#[derive(Clone, Copy)]
enum State<'d> {
    Vacant,
    Active(Publication<'d>),
    Suspended(Publication<'d>),
    Retired,
}

#[derive(Clone, Copy)]
struct Transaction<'d> {
    publication: Publication<'d>,
    generation: u64,
}

struct Suspension<'a, 'd> {
    entry: &'a Entry<'d>,
    transaction: Transaction<'d>,
    armed: bool,
}

impl<'a, 'd> Suspension<'a, 'd> {
    fn begin(entry: &'a Entry<'d>, transaction: Transaction<'d>) -> Option<Self> {
        entry.suspend(transaction).then_some(Self {
            entry,
            transaction,
            armed: true,
        })
    }

    fn restore(mut self) -> bool {
        self.armed = false;
        self.entry.restore(self.transaction)
    }
}

impl Drop for Suspension<'_, '_> {
    fn drop(&mut self) {
        if self.armed {
            self.entry.restore(self.transaction);
        }
    }
}

impl<'d> Entry<'d> {
    fn new(lane: usize) -> Self {
        Self {
            state: Cell::new(State::Vacant),
            lane,
            close: Cell::new(false),
            generation: Cell::new(0),
        }
    }

    fn matches_token(current: Token, expected: Token) -> bool {
        current.same_target(expected)
    }

    fn matches_publication(current: Publication<'d>, expected: Publication<'d>) -> bool {
        Self::matches_token(current.token, expected.token) && current.ready == expected.ready
    }

    fn transaction(&self, token: Token) -> Option<Transaction<'d>> {
        let State::Active(publication) = self.state.get() else {
            return None;
        };
        Self::matches_token(publication.token, token).then(|| Transaction {
            publication,
            generation: self.generation.get(),
        })
    }

    fn is_active(&self, transaction: Transaction<'d>) -> bool {
        if self.generation.get() != transaction.generation {
            return false;
        }
        matches!(
            self.state.get(),
            State::Active(publication)
                if Self::matches_publication(publication, transaction.publication)
        )
    }

    fn is_suspended(&self, transaction: Transaction<'d>) -> bool {
        if self.generation.get() != transaction.generation {
            return false;
        }
        matches!(
            self.state.get(),
            State::Suspended(publication)
                if Self::matches_publication(publication, transaction.publication)
        )
    }

    fn suspend(&self, transaction: Transaction<'d>) -> bool {
        if !self.is_active(transaction) {
            return false;
        }
        self.state.set(State::Suspended(transaction.publication));
        true
    }

    fn restore(&self, transaction: Transaction<'d>) -> bool {
        if !self.is_suspended(transaction) {
            return false;
        }
        self.state.set(State::Active(transaction.publication));
        true
    }

    fn advance_generation(&self) -> Option<u64> {
        let next = self.generation.get().checked_add(1)?;
        self.generation.set(next);
        Some(next)
    }

    fn retire<B>(&self, arena: &metadata::Arena<'d, B>, token: &mut RegionToken<'d>) {
        self.state.set(State::Retired);
        self.close.set(false);
        arena.clear(self.lane, token);
    }

    fn mark_ready(&self, transaction: Transaction<'d>, driver: DriverRef<'d>) {
        if self.is_active(transaction) {
            driver.activate_ready(transaction.publication.ready);
        }
    }

    fn deactivate<B>(
        &self,
        token: Token,
        arena: &metadata::Arena<'d, B>,
        region: &mut RegionToken<'d>,
    ) {
        match self.state.get() {
            State::Active(publication) | State::Suspended(publication)
                if Self::matches_token(publication.token, token) => {}
            State::Vacant | State::Active(_) | State::Suspended(_) | State::Retired => return,
        }
        if self.advance_generation().is_some() {
            self.state.set(State::Vacant);
        } else {
            self.state.set(State::Retired);
        }
        self.close.set(false);
        arena.clear(self.lane, region);
    }

    fn close<B>(
        &self,
        token: Token,
        arena: &metadata::Arena<'d, B>,
        region: &mut RegionToken<'d>,
        driver: DriverRef<'d>,
    ) {
        let (publication, suspended) = match self.state.get() {
            State::Active(publication) if Self::matches_token(publication.token, token) => {
                (publication, false)
            }
            State::Suspended(publication) if Self::matches_token(publication.token, token) => {
                (publication, true)
            }
            State::Vacant | State::Active(_) | State::Suspended(_) | State::Retired => return,
        };
        if suspended {
            if self.advance_generation().is_none() {
                self.retire(arena, region);
                driver.activate_ready(publication.ready);
                return;
            }
            self.state.set(State::Active(publication));
        }
        self.close.set(true);
        driver.activate_ready(publication.ready);
    }

    fn close_transaction(&self, transaction: Transaction<'d>, driver: DriverRef<'d>) {
        if self.is_active(transaction) {
            self.close.set(true);
            driver.activate_ready(transaction.publication.ready);
        }
    }
}

pub struct Port<'d, B> {
    driver: DriverRef<'d>,
    arena: metadata::Arena<'d, B>,
    entries: Box<[Entry<'d>]>,
}

pub struct Sender<'a, 'd, B> {
    driver: DriverRef<'d>,
    arena: &'a metadata::Arena<'d, B>,
    entry: &'a Entry<'d>,
    transaction: Transaction<'d>,
}

pub struct Receiver<'a, 'd, B> {
    arena: &'a metadata::Arena<'d, B>,
    entry: &'a Entry<'d>,
    transaction: Transaction<'d>,
}

impl<'a, 'd, B: StableBytes> Sender<'a, 'd, B> {
    pub fn try_enqueue(&self, token: &mut RegionToken<'d>, value: B) -> Result<(), B> {
        let Some(suspension) = Suspension::begin(self.entry, self.transaction) else {
            return Err(value);
        };
        let bytes = value.as_ref().len();
        if !suspension.restore() {
            return Err(value);
        }
        if bytes == 0 {
            drop(value);
            return Ok(());
        }
        self.arena
            .queue(self.entry.lane)
            .try_push_back(token, value, bytes)?;
        self.entry.mark_ready(self.transaction, self.driver);
        Ok(())
    }

    pub fn close(&self) {
        self.entry.close_transaction(self.transaction, self.driver);
    }
}

impl<'d, B: StableBytes> Port<'d, B> {
    pub fn with_capacity(capacity: usize, token: &RegionToken<'d>, driver: DriverRef<'d>) -> Self {
        Self::with_egress(capacity, token, Config::default(), driver)
    }

    pub fn with_egress(
        capacity: usize,
        token: &RegionToken<'d>,
        config: Config,
        driver: DriverRef<'d>,
    ) -> Self {
        let arena = metadata::Arena::with_config(token, config, capacity.max(1));
        Self {
            driver,
            arena,
            entries: (0..capacity).map(Entry::new).collect(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub fn is_active(&self, token: Token) -> bool {
        self.entry_transaction(token).is_some()
    }

    pub fn with_sender<R>(
        &self,
        token: Token,
        f: impl for<'a> FnOnce(Sender<'a, 'd, B>) -> R,
    ) -> Option<R> {
        let (entry, transaction) = self.entry_transaction(token)?;
        Some(f(Sender {
            driver: self.driver,
            arena: &self.arena,
            entry,
            transaction,
        }))
    }

    pub fn with_receiver<R>(
        &self,
        token: Token,
        f: impl for<'a> FnOnce(Receiver<'a, 'd, B>) -> R,
    ) -> Option<R> {
        let (entry, transaction) = self.entry_transaction(token)?;
        Some(f(Receiver {
            arena: &self.arena,
            entry,
            transaction,
        }))
    }

    fn entry_transaction(&self, token: Token) -> Option<(&Entry<'d>, Transaction<'d>)> {
        let entry = self.entries.get(token.slot().raw() as usize)?;
        Some((entry, entry.transaction(token)?))
    }

    pub fn activate(
        &self,
        region: &mut RegionToken<'d>,
        token: Token,
        ready: ReadyKey<'d>,
    ) -> bool {
        let Some(entry) = self.entries.get(token.slot().raw() as usize) else {
            return false;
        };
        let Some(generation) = entry.advance_generation() else {
            entry.retire(&self.arena, region);
            return false;
        };
        entry.state.set(State::Vacant);
        entry.close.set(false);
        let queue = self.arena.queue(entry.lane);
        let detached = queue.detach_all(region);
        let transaction = Transaction {
            publication: Publication { token, ready },
            generation,
        };
        entry.state.set(State::Active(transaction.publication));
        drop(detached);
        entry.is_active(transaction)
    }

    pub fn deactivate(&self, region: &mut RegionToken<'d>, token: Token) {
        if let Some(entry) = self.entries.get(token.slot().raw() as usize) {
            entry.deactivate(token, &self.arena, region);
        }
    }

    pub fn try_enqueue(
        &self,
        region: &mut RegionToken<'d>,
        token: Token,
        value: B,
    ) -> Result<(), B> {
        let Some((entry, transaction)) = self.entry_transaction(token) else {
            return Err(value);
        };
        Sender {
            driver: self.driver,
            arena: &self.arena,
            entry,
            transaction,
        }
        .try_enqueue(region, value)
    }

    pub fn close(&self, region: &mut RegionToken<'d>, token: Token) {
        if let Some(entry) = self.entries.get(token.slot().raw() as usize) {
            entry.close(token, &self.arena, region, self.driver);
        }
    }

    pub fn drain_requests(
        &self,
        region: &mut RegionToken<'d>,
        token: Token,
        push: impl FnMut(&mut RegionToken<'d>, B) -> Result<(), B>,
    ) -> Option<Requests> {
        self.with_receiver(token, |receiver| {
            receiver.drain(region, push);
            Requests {
                close: receiver.take_close().then_some(CloseKind::Reconnect),
            }
        })
    }
}

impl<'a, 'd, B: StableBytes> Receiver<'a, 'd, B> {
    pub fn drain(
        &self,
        token: &mut RegionToken<'d>,
        mut push: impl FnMut(&mut RegionToken<'d>, B) -> Result<(), B>,
    ) {
        let Some(suspension) = Suspension::begin(self.entry, self.transaction) else {
            return;
        };
        loop {
            let queue = self.arena.queue(self.entry.lane);
            let Some((value, mut front)) = queue.take_front(token) else {
                suspension.restore();
                return;
            };
            match front.with_region(|region| push(region, value)) {
                Ok(()) => {
                    front.release();
                    if !self.entry.is_suspended(self.transaction) {
                        return;
                    }
                }
                Err(value) if self.entry.is_suspended(self.transaction) => {
                    front.restore(value);
                    suspension.restore();
                    return;
                }
                Err(value) => {
                    front.release();
                    drop(value);
                    return;
                }
            }
        }
    }

    pub fn take_close(&self) -> bool {
        if self.entry.is_active(self.transaction) {
            self.entry.close.take()
        } else {
            false
        }
    }
}
