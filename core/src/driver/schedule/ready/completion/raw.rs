use std::{marker, pin};

use o3;
use ready::task;

use crate::driver::{self, schedule::ready};

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Waker<'d> {
    driver: driver::Reference<'d>,
    key: ready::Key<'d>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    _thread: o3::ThreadBound,
}

impl<'d> Waker<'d> {
    pub(super) fn from_ready(driver: driver::Reference<'d>, key: ready::Key<'d>) -> Self {
        Self {
            driver,
            key,
            _driver: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }

    pub(super) fn same_target(&self, other: &Self) -> bool {
        self.driver.same_driver(other.driver) && self.key == other.key
    }

    pub(in crate::driver::schedule::ready) fn same_driver(&self, other: &Self) -> bool {
        self.driver.same_driver(other.driver)
    }

    /// # Safety
    /// This waker must originate from a root target and therefore be unable to
    /// name a task entry, even if its generation has since become stale.
    pub(in crate::driver::schedule::ready) unsafe fn into_root_target(self) -> ready::Target<'d> {
        ready::Target::new(self.driver, self.key)
    }

    pub(in crate::driver::schedule::ready) fn task_hops_at_most(self, maximum: usize) -> bool {
        let driver = self.driver;
        let mut key = self.key;
        let mut remaining = maximum;

        loop {
            let parent = ready::Access::with(&driver, |access| {
                driver.ready().arena().task_parent(access, key)
            });
            match parent {
                ready::TaskParent::Root => return true,
                ready::TaskParent::Task(parent) => {
                    if remaining == 0 {
                        return false;
                    }
                    remaining -= 1;
                    key = parent.0.key;
                }
                ready::TaskParent::Stale => return false,
            }
        }
    }

    pub(in crate::driver::schedule::ready) fn lease_tasks(
        self,
        requested: usize,
    ) -> Result<(), usize> {
        self.driver
            .ready()
            .arena()
            .entries
            .pool
            .lease_tasks(requested)
    }

    pub(in crate::driver::schedule::ready) fn reserve_task(self) -> Option<ready::Reservation<'d>> {
        let entries = &self.driver.ready().arena().entries;
        entries.pool.reserve_task(&entries.slots).ok()
    }

    pub(in crate::driver::schedule::ready) fn claim_leased_task(
        self,
    ) -> Option<ready::Reservation<'d>> {
        let entries = &self.driver.ready().arena().entries;
        entries.pool.claim_leased_task(&entries.slots)
    }

    pub(in crate::driver::schedule::ready) fn release_task_lease(self, remaining: usize) {
        self.driver
            .ready()
            .arena()
            .entries
            .pool
            .release_task_lease(remaining);
    }

    pub(in crate::driver::schedule::ready) fn return_leased_task(
        self,
        reservation: &ready::Reservation<'d>,
    ) {
        let entries = &self.driver.ready().arena().entries;
        entries.pool.return_leased_task(&entries.slots, reservation);
    }

    /// # Safety
    /// The node must remain pinned until its installed key is released.
    pub(in crate::driver::schedule::ready) unsafe fn install_task(
        self,
        reservation: ready::Reservation<'d>,
        node: pin::Pin<&task::Node<'d>>,
    ) -> ready::DynamicKey<'d> {
        let task = unsafe { ready::raw::Task::new(node) };
        let arena = self.driver.ready().arena();
        arena
            .entries
            .pool
            .install_task(&arena.entries.slots, reservation, task)
    }

    pub(in crate::driver::schedule::ready) fn release_admission(
        self,
        reservation: &ready::Reservation<'d>,
    ) {
        let entries = &self.driver.ready().arena().entries;
        entries.pool.release_reserved(&entries.slots, reservation);
    }

    pub(in crate::driver::schedule::ready) fn retarget(self, key: ready::Key<'d>) -> Self {
        Self::from_ready(self.driver, key)
    }

    pub(in crate::driver::schedule::ready) fn release(self, key: ready::DynamicKey<'d>) -> bool {
        self.driver.ready().arena().release(key)
    }

    pub(in crate::driver::schedule::ready) fn reclaim_task(
        self,
        key: ready::DynamicKey<'d>,
    ) -> (Option<ready::Reservation<'d>>, bool) {
        self.driver.ready().arena().reclaim_task(key)
    }

    pub(super) fn wake(self) {
        self.driver.ready().activate_ready(self.key);
    }
}
