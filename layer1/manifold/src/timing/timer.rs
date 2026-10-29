use std::task;

use dope_core::driver::{
    self, route,
    route::{Identity, indices},
    schedule::{
        clock::{self, timer},
        ready,
        ready::wake,
    },
};

use crate::app::creation;

pub(crate) struct Timer<'d, ID: Identity> {
    alarm: timer::Alarm<'d>,
    ready: ready::Slot<'d, ID>,
}

impl<'d, ID: Identity> Timer<'d, ID> {
    pub(crate) fn new(driver: &mut driver::Context<'_, 'd>) -> Result<Self, creation::Error> {
        let owner = driver.driver_ref();
        let target = owner
            .targets::<ID>()
            .bind(indices::SlotIndex::ZERO, route::Epoch::INITIAL);
        let ready = owner
            .ready()
            .make_slot(target.dispatch())
            .map_err(creation::Error::Ready)?;
        Ok(Self {
            alarm: driver
                .driver_ref()
                .timer()
                .try_alarm()
                .map_err(creation::Error::Timer)?,
            ready,
        })
    }

    pub(crate) fn poll(&self, now: clock::Moment<'d>) -> task::Poll<()> {
        let wake = wake::Waker::from(self.ready.target());
        self.alarm.poll(now, wake)
    }

    pub(crate) fn arm(&self, deadline: clock::Deadline<'d>) {
        let wake = wake::Waker::from(self.ready.target());
        self.alarm.arm(deadline, wake);
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.alarm.is_armed()
    }

    pub(crate) fn cancel(&self) {
        self.alarm.clear();
    }
}
