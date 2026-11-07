use dope_core::driver::route::{indices, table};

use crate::client::side::registry;

pub(in crate::client) enum Start {
    Started,
    InProgress,
}

#[derive(Clone, Copy)]
pub(super) enum Step {
    Done,
    Intake,
    Lane(indices::SlotIndex),
}

#[derive(Clone, Copy)]
pub(super) enum Primary {
    Done,
    Lane(indices::SlotIndex),
}

pub(in crate::client) enum State {
    Open,
    Draining(Cursor),
    Done,
}

pub(in crate::client) struct Cursor {
    lane: Option<indices::SlotIndex>,
    next: Next,
}

#[derive(Clone, Copy)]
enum Next {
    Intake,
    Lane,
}

#[must_use = "a shutdown plan must be completed only after work is admitted"]
pub(super) enum Plan<'a> {
    Intake(&'a mut Cursor),
    Lane(
        &'a mut Cursor,
        indices::SlotIndex,
        Option<indices::SlotIndex>,
    ),
}

impl Cursor {
    pub(super) fn intake(&mut self) {
        self.next = Next::Lane;
    }

    pub(super) fn advance(&mut self, next: Option<indices::SlotIndex>) {
        self.lane = next;
        self.next = Next::Intake;
    }
}

impl State {
    pub(in crate::client) const fn new() -> Self {
        Self::Open
    }

    pub(in crate::client) fn begin(&mut self) -> Start {
        if !matches!(self, Self::Open) {
            return Start::InProgress;
        }
        *self = Self::Draining(Cursor {
            lane: Some(indices::SlotIndex::ZERO),
            next: Next::Intake,
        });
        Start::Started
    }

    pub(in crate::client) const fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }

    pub(in crate::client) const fn is_draining(&self) -> bool {
        matches!(self, Self::Draining(_))
    }

    pub(super) fn plan(
        &mut self,
        queue: registry::Queue,
        capacity: table::Capacity,
    ) -> Option<Plan<'_>> {
        let step = match self {
            Self::Draining(cursor) => {
                match (cursor.lane.filter(|lane| capacity.contains(*lane)), queue) {
                    (None, registry::Queue::Empty) => Step::Done,
                    (None, registry::Queue::Pending) => Step::Intake,
                    (Some(_), registry::Queue::Pending) if matches!(cursor.next, Next::Intake) => {
                        Step::Intake
                    }
                    (Some(lane), registry::Queue::Empty | registry::Queue::Pending) => {
                        Step::Lane(lane)
                    }
                }
            }
            Self::Open | Self::Done => return None,
        };
        if matches!(step, Step::Done) {
            *self = Self::Done;
            return None;
        }
        match (self, step) {
            (Self::Draining(cursor), Step::Intake) => Some(Plan::Intake(cursor)),
            (Self::Draining(cursor), Step::Lane(lane)) => {
                Some(Plan::Lane(cursor, lane, capacity.next(lane)))
            }
            (Self::Open | Self::Done, _) | (Self::Draining(_), Step::Done) => None,
        }
    }

    pub(super) fn next_lane(&mut self, capacity: table::Capacity) -> Option<Primary> {
        let Self::Draining(cursor) = self else {
            return None;
        };
        match cursor.lane.filter(|lane| capacity.contains(*lane)) {
            Some(lane) => {
                cursor.lane = capacity.next(lane);
                Some(Primary::Lane(lane))
            }
            None => {
                *self = Self::Done;
                Some(Primary::Done)
            }
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Option<indices::SlotIndex>>() == std::mem::size_of::<indices::SlotIndex>()
);
const _: () = assert!(std::mem::size_of::<State>() == 2 * std::mem::size_of::<u32>());
