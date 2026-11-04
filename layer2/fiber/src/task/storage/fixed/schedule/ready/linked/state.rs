use core::{cell, pin::Pin};

use crate::task::storage::fixed::schedule::ready::linked::{Entry, Side, Store};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Mode {
    IdleFirst,
    IdleSecond,
    PausedFirst,
    PausedSecond,
}

pub(in crate::task::storage::fixed::schedule) struct State {
    pub(in crate::task::storage::fixed::schedule::ready::linked) store: Store,
    mode: cell::Cell<Mode>,
}

impl State {
    pub(in crate::task::storage::fixed::schedule::ready::linked) fn new() -> Self {
        Self {
            store: Store::default(),
            mode: cell::Cell::new(Mode::IdleFirst),
        }
    }

    pub(in crate::task::storage::fixed::schedule) fn enqueue(&self, entry: Pin<&Entry>) {
        self.store.enqueue(self.active(), entry);
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn is_empty(&self) -> bool {
        self.store.is_empty(Side::First) && self.store.is_empty(Side::Second)
    }

    fn active(&self) -> Side {
        match self.mode.get() {
            Mode::IdleFirst => Side::First,
            Mode::IdleSecond => Side::Second,
            Mode::PausedFirst => Side::Second,
            Mode::PausedSecond => Side::First,
        }
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn start(&self) -> Side {
        match self.mode.get() {
            Mode::PausedFirst => {
                if self.store.is_empty(Side::First) {
                    self.active()
                } else {
                    Side::First
                }
            }
            Mode::PausedSecond => {
                if self.store.is_empty(Side::Second) {
                    self.active()
                } else {
                    Side::Second
                }
            }
            Mode::IdleFirst | Mode::IdleSecond => self.active(),
        }
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn pause(&self, side: Side) {
        if self.store.is_empty(side) {
            self.seal(side);
        } else {
            self.paused(side);
        }
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn seal(&self, side: Side) {
        let pending = side.other();
        if self.store.is_empty(pending) {
            self.idle(pending);
        } else {
            self.paused(pending);
        }
    }

    fn idle(&self, side: Side) {
        self.mode.set(match side {
            Side::First => Mode::IdleFirst,
            Side::Second => Mode::IdleSecond,
        });
    }

    fn paused(&self, side: Side) {
        self.mode.set(match side {
            Side::First => Mode::PausedFirst,
            Side::Second => Mode::PausedSecond,
        });
    }
}
