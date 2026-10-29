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
    Cancel,
}

#[derive(Clone, Copy)]
pub(super) enum Primary {
    Done,
    Lane(indices::SlotIndex),
}

pub(in crate::client) enum State {
    Open,
    Draining(Cursor),
    Cancelling(u32),
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
    Cancel(&'a mut u32),
}

#[must_use = "a primary shutdown plan must be completed only after work is admitted"]
pub(super) enum PrimaryPlan<'a> {
    Done(&'a mut State),
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

impl PrimaryPlan<'_> {
    pub(super) fn complete(self) -> Primary {
        match self {
            Self::Done(state) => {
                *state = State::Done;
                Primary::Done
            }
            Self::Lane(cursor, lane, next) => {
                cursor.lane = next;
                Primary::Lane(lane)
            }
        }
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
        matches!(self, Self::Draining(_) | Self::Cancelling(_))
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
            Self::Cancelling(_) => Step::Cancel,
            Self::Open | Self::Done => return None,
        };
        if matches!(step, Step::Done) {
            *self = Self::Cancelling(capacity.count().raw());
        }
        match (self, step) {
            (Self::Cancelling(cursor), _) => Some(Plan::Cancel(cursor)),
            (Self::Draining(cursor), Step::Intake) => Some(Plan::Intake(cursor)),
            (Self::Draining(cursor), Step::Lane(lane)) => {
                Some(Plan::Lane(cursor, lane, capacity.next(lane)))
            }
            (Self::Open | Self::Done, _) | (Self::Draining(_), Step::Done | Step::Cancel) => None,
        }
    }

    pub(super) fn plan_lane(&mut self, capacity: table::Capacity) -> Option<PrimaryPlan<'_>> {
        let step = match self {
            Self::Draining(cursor) => cursor
                .lane
                .filter(|lane| capacity.contains(*lane))
                .map_or(Primary::Done, Primary::Lane),
            Self::Open | Self::Cancelling(_) | Self::Done => return None,
        };
        match (self, step) {
            (state, Primary::Done) => Some(PrimaryPlan::Done(state)),
            (Self::Draining(cursor), Primary::Lane(lane)) => {
                Some(PrimaryPlan::Lane(cursor, lane, capacity.next(lane)))
            }
            (Self::Open | Self::Cancelling(_) | Self::Done, Primary::Lane(_)) => None,
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Option<indices::SlotIndex>>() == std::mem::size_of::<indices::SlotIndex>()
);
const _: () = assert!(std::mem::size_of::<State>() == 2 * std::mem::size_of::<u32>());
