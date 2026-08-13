use std::{io, pin, task};

use dope_core::driver::{
    self, route,
    schedule::{ready, ready::completion, timer},
};

#[pin_project::pin_project(!Unpin)]
pub(super) struct Timer<'d, const ID: u8> {
    #[pin]
    registration: timer::Registration<'d, 'd>,
    ready: ready::Slot<'d, route::KeyTag<ID>>,
}

impl<'d, const ID: u8> Timer<'d, ID> {
    pub(super) fn new(driver: &mut driver::Context<'_, 'd>) -> io::Result<Self> {
        let reference = driver.driver_ref();
        let target = route::Space::<route::KeyTag<ID>>::for_driver(reference)
            .bind(route::SlotIndex::ZERO, route::Epoch::INITIAL);
        let ready = reference.ready().make_ready_slot(target.dispatch())?;
        Ok(Self {
            registration: timer::Registration::new(driver.timer()),
            ready,
        })
    }

    pub(super) fn poll(
        self: pin::Pin<&Self>,
        now: timer::Deadline<'d>,
        driver: driver::Reference<'d>,
    ) -> task::Poll<()> {
        let this = self.project_ref();
        let wake = completion::Waker::from_ready(driver, this.ready.key());
        this.registration.poll(now, wake)
    }

    pub(super) fn arm(
        self: pin::Pin<&Self>,
        deadline: timer::Deadline<'d>,
        driver: driver::Reference<'d>,
    ) {
        let this = self.project_ref();
        let wake = completion::Waker::from_ready(driver, this.ready.key());
        this.registration.arm(deadline, wake);
    }

    pub(super) fn is_armed(self: pin::Pin<&Self>) -> bool {
        self.project_ref().registration.is_armed()
    }

    pub(super) fn cancel(self: pin::Pin<&Self>) -> bool {
        self.project_ref().registration.cancel()
    }
}
