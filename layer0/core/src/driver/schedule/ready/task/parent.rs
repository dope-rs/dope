use crate::{
    driver,
    driver::schedule::{ready, ready::task},
};

/// Reusable parent capability for one exact ready node.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Parent<'d>(pub(in crate::driver::schedule::ready::task) ready::Address<'d>);

impl<'d> Parent<'d> {
    pub fn wake(self) {
        self.0.wake();
    }

    #[doc(hidden)]
    pub fn belongs_to(self, driver: driver::Reference<'_>) -> bool {
        self.0.belongs_to(driver)
    }

    pub(in crate::driver::schedule::ready) fn address(self) -> ready::Address<'d> {
        self.0
    }

    pub(in crate::driver::schedule::ready::task) fn driver(self) -> driver::Reference<'d> {
        self.0.driver
    }

    pub(in crate::driver::schedule::ready::task) fn key(self) -> ready::Key<'d> {
        self.0.key()
    }

    pub(in crate::driver::schedule::ready::task) fn validate(
        self,
        max: usize,
    ) -> Result<(), task::Error> {
        let address = self.0;
        let driver = address.driver;
        let mut key = address.key();
        let mut hops = max;
        let access = ready::Access::new(&driver);

        loop {
            match driver.ready().arena().task_parent(access, key) {
                ready::TaskParent::Root => return Ok(()),
                ready::TaskParent::Task(parent) => {
                    if hops == 0 {
                        return Err(task::Error::WakeHopCeiling {
                            minimum: max + 1,
                            maximum: max,
                        });
                    }
                    hops -= 1;
                    key = parent;
                }
                ready::TaskParent::Stale => return Err(task::Error::ParentStale),
            }
        }
    }

    /// Admits one exact child after validating the complete wake chain and capacity.
    pub fn try_admit(self) -> Result<task::raw::Admission<'d>, task::Error> {
        self.validate(task::MAX_WAKE_HOPS - 1)?;
        let arena = self.0.driver.ready().arena();
        let vacant = arena.entries.pool.prepare_task(&arena.entries.slots)?;
        let child = vacant.commit();
        Ok(task::raw::Admission::new(self, child))
    }

    pub(in crate::driver::schedule::ready) fn return_lease(
        self,
        held: ready::Permit<'d>,
        chain: &ready::Chain,
    ) {
        let entries = &self.0.driver.ready().arena().entries;
        chain.restore(&entries.slots, held.into_key());
    }

    pub(in crate::driver::schedule::ready) fn install_task(
        self,
        held: ready::Permit<'d>,
        root: &ready::TopologyKey<'d>,
        local: u32,
    ) -> ready::TaskKey<'d> {
        let arena = self.0.driver.ready().arena();
        let task = ready::Task::new(self.key(), root, local);
        held.install(&arena.entries.slots, root, task)
    }

    pub(in crate::driver::schedule::ready) fn release_hold(self, held: ready::Permit<'d>) {
        let entries = &self.0.driver.ready().arena().entries;
        entries.pool.recycle(&entries.slots, held.into_key());
    }

    pub(in crate::driver::schedule::ready) fn retarget(self, key: ready::Key<'d>) -> Self {
        Self(self.0.retarget(key))
    }

    pub(in crate::driver::schedule::ready) fn release(self, key: ready::TaskKey<'d>) {
        self.0.driver.ready().arena().free_task(key)
    }

    pub(in crate::driver::schedule::ready) fn reclaim_task(
        self,
        key: ready::TaskKey<'d>,
    ) -> ready::Reclaimed<'d> {
        self.0.driver.ready().arena().reclaim_task(key)
    }
}

impl<'d> From<ready::Target<'d>> for Parent<'d> {
    fn from(target: ready::Target<'d>) -> Self {
        Self(target.address())
    }
}

const _: () = {
    assert!(std::mem::size_of::<Parent<'static>>() == 2 * std::mem::size_of::<usize>());
};
