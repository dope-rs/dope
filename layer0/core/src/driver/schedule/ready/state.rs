use std::{cell, mem};

use o3::base::num::Generation;

use crate::driver::schedule::ready;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Lane {
    First,
    Second,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Value {
    generation: Generation,
    kind: ready::Kind,
    queue: Option<Lane>,
    padding: [u8; 2],
}

#[repr(transparent)]
pub(super) struct State(cell::Cell<Value>);

#[must_use = "a vacant queue state must be queued or left vacant"]
pub(super) struct Vacant<'a>(&'a State);

#[must_use = "a queued state must be removed from its exact lane"]
pub(super) struct Queued<'a> {
    state: &'a State,
    lane: Lane,
}

pub(in crate::driver::schedule::ready) enum Any {}
pub(super) enum Free {}
pub(super) enum Reserved {}
pub(super) enum Dispatch {}
pub(super) enum Task {}
pub(super) enum Topology {}

impl Lane {
    pub(super) const fn index(self) -> usize {
        self as usize
    }

    pub(super) const fn raw(self) -> u8 {
        self as u8
    }

    pub(super) const fn flip(self) -> Self {
        match self {
            Self::First => Self::Second,
            Self::Second => Self::First,
        }
    }

    pub(super) const fn from_bit(second: bool) -> Self {
        if second { Self::Second } else { Self::First }
    }
}

impl State {
    pub(super) const fn new(epoch: Generation, kind: ready::Kind) -> Self {
        Self(cell::Cell::new(Value {
            generation: epoch,
            kind,
            queue: None,
            padding: [0; 2],
        }))
    }

    pub(super) fn generation(&self) -> Generation {
        self.0.get().generation
    }

    pub(super) fn kind(&self) -> ready::Kind {
        self.0.get().kind
    }

    pub(super) fn set_epoch(&self, epoch: Generation) {
        let mut value = self.0.get();
        value.generation = epoch;
        self.0.set(value);
    }

    pub(super) fn set_kind(&self, kind: ready::Kind) {
        let mut value = self.0.get();
        value.kind = kind;
        value.queue = None;
        self.0.set(value);
    }

    pub(super) fn vacant(&self) -> Option<Vacant<'_>> {
        self.0.get().queue.is_none().then_some(Vacant(self))
    }

    pub(super) fn queued(&self) -> Option<Queued<'_>> {
        self.0.get().queue.map(|lane| Queued { state: self, lane })
    }

    pub(super) fn bind(&self, lane: Lane) -> Queued<'_> {
        Queued { state: self, lane }
    }
}

impl Vacant<'_> {
    pub(super) fn queue(self, lane: Lane) {
        let mut value = self.0.0.get();
        value.queue = Some(lane);
        self.0.0.set(value);
    }
}

impl Queued<'_> {
    pub(super) const fn lane(&self) -> Lane {
        self.lane
    }

    pub(super) fn clear(self) {
        let mut value = self.state.0.get();
        value.queue = None;
        self.state.0.set(value);
    }
}

const _: () = {
    assert!(mem::size_of::<Value>() == mem::size_of::<u64>());
    assert!(mem::size_of::<State>() == mem::size_of::<u64>());
};
