use std::{cell, io, time};

use crate::driver::{
    self,
    schedule::{ready, timer},
    settings,
};

pub(super) struct State {
    pub(super) arena: Box<ready::Arena>,
    clock: cell::Cell<time::Instant>,
}

impl State {
    pub(super) fn try_new(
        file_slots: settings::FileSlots,
        dynamic_slots: settings::ScheduleCapacity,
    ) -> io::Result<Self> {
        Ok(Self {
            arena: ready::Arena::new(file_slots.table_capacity().get(), dynamic_slots)?,
            clock: cell::Cell::new(time::Instant::now()),
        })
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Scheduler<'d>(driver::Reference<'d>);

impl<'d> Scheduler<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub fn deadline(self, at: time::Instant) -> timer::Deadline<'d> {
        timer::Deadline::new(at)
    }

    pub fn turn_now(self) -> time::Instant {
        self.0.shared.scheduling.clock.get()
    }

    pub(in crate::driver) fn set_turn_now(self, now: time::Instant) {
        self.0.shared.scheduling.clock.set(now);
    }
}

const _: () = assert!(
    std::mem::size_of::<Scheduler<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);
