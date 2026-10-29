use dope_sys::io::socket::addr::{Length, Storage};

use crate::{
    driver::{
        flight, retained,
        retained::{accept, arm},
        route,
        route::kind,
    },
    io::fd::handles,
};

#[must_use]
pub struct Arming<'a, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    entry: accept::Entry<'a, 'd, Tag>,
}

impl<'a, 'd, Tag> Arming<'a, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    pub(super) fn new(entry: accept::Entry<'a, 'd, Tag>) -> Self {
        Self { entry }
    }

    /// Defers admission until prior descriptor retirements return.
    /// # Safety
    /// `driver` must retain this pinned lane through terminal completion or owner quiescence.
    pub unsafe fn submit<'owner>(
        mut self,
        driver: &mut retained::Context<'_, 'owner, 'd>,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
    ) -> arm::Submission
    where
        'owner: 'a,
    {
        if !matches!(
            accept::Readiness::from(driver.driver_ref()),
            accept::Readiness::Ready
        ) {
            return arm::Submission::Deferred;
        }
        let lane = self.entry.lane.as_mut().project();
        let peer = lane.addr.get_mut();
        *peer = Storage::<Length>::empty();
        let closed = driver.driver_ref().descriptors().retirement().completed();
        let request = retained::raw::Submission::accept(fd, peer, lane.state.operation());
        // SAFETY: the caller retains this exact pinned lane through terminal
        // completion or owner quiescence.
        let flight = unsafe { request.try_submit(driver.reborrow(), slots) };
        self.entry.apply(flight, closed)
    }
}
