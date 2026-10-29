use o3::mem::quota;
pub(in crate::driver::schedule::ready) mod cells;
pub(in crate::driver::schedule::ready) mod layout;
pub(in crate::driver::schedule::ready) mod payload;
pub(in crate::driver::schedule::ready) mod pool;
pub(in crate::driver::schedule::ready) mod set;
pub(in crate::driver::schedule::ready) mod topology;

use o3::{alloc, depot::queue::Removal};
use state::lanes;

use crate::{
    dope_sys::Budget as _,
    driver,
    driver::{
        receive,
        receive::credit,
        route,
        route::kind,
        schedule::{ready, state},
        settings,
    },
    io::fd::{domain, identity},
};

pub(crate) struct Arena {
    descriptors: domain::Domain,
    ready: set::Set,
    pub(super) entries: ready::Table,
}

impl Arena {
    pub(in crate::driver) fn new(plan: settings::Plan) -> Result<Self, alloc::Error> {
        let layout = ready::Layout::new(plan);
        Ok(Self {
            descriptors: domain::Domain::try_from(plan.config().file_slots())?,
            ready: set::Set::try_new(plan.ready())?,
            entries: ready::Table::try_new(layout)?,
        })
    }

    pub(crate) const fn descriptors(&self) -> &domain::Domain {
        &self.descriptors
    }

    pub(crate) fn unqueue_fd(&self, slot: &impl ready::QueueIndex) {
        self.ready.clear(slot.queue_index());
    }

    pub(crate) fn activate_fd(&self, id: identity::Identity<'_>) {
        self.ready.insert(ready::QueueIndex::queue_index(&id));
    }

    pub(crate) fn make_slot<'d, Tag: route::Tag>(
        &'d self,
        driver: driver::Reference<'d>,
        target: route::Operation<'d, Tag>,
    ) -> Result<ready::Slot<'d, Tag>, ready::Capacity> {
        let key = self
            .entries
            .pool
            .try_dispatch(&self.entries.slots, target.into_token())?;
        Ok(ready::Slot::new(driver, key))
    }

    pub(in crate::driver) fn activate<'a, 'd>(
        &'a self,
        access: ready::Access<'a, 'd>,
        key: ready::Key<'d>,
    ) where
        'd: 'a,
    {
        self.stored().complete(access, ready::Retained::new(key));
    }

    fn resolve(&self, key: ready::Stored) -> Option<ready::Entry<'_>> {
        let index = key.dense();
        if let Some(slot) = self.entries.slots.descriptor(index) {
            return self
                .descriptors
                .target_for(slot, key.epoch())
                .map(|_| ready::Entry::Dispatch(index));
        }
        self.entries.slots.resolve(key)
    }

    fn queue_entry<'a, 'd>(&'a self, access: ready::Access<'a, 'd>, entry: ready::Entry<'a>)
    where
        'd: 'a,
    {
        use ready::task::Activation;

        match entry {
            ready::Entry::Task(task, index) => {
                let record = task.record();
                let topology = ready::Topology::at(&self.entries.slots, record.topology());
                if topology.activate(index) == Activation::Queued {
                    self.activate(access, access.bind(record.parent()));
                }
            }
            ready::Entry::Dispatch(index) => self.ready.insert(index),
        }
    }

    pub(in crate::driver) const fn stored(&self) -> Stored<'_> {
        Stored(self)
    }

    pub(super) fn free_route<'d>(&'d self, key: ready::DispatchKey<'d>) {
        let index = key.key().raw().dense();
        let held = self.entries.slots.mutation().unroute(key);
        self.ready.clear(index);
        self.entries.pool.recycle(&self.entries.slots, held);
    }

    pub(super) fn free_task<'d>(&'d self, key: ready::TaskKey<'d>) {
        self.entries.pool.free_task(&self.entries.slots, key)
    }

    pub(super) fn reclaim_task<'d>(&'d self, key: ready::TaskKey<'d>) -> ready::Reclaimed<'d> {
        let ready::Detached { task, root } = key.detach(&self.entries.slots);
        if let Some(root) = root {
            self.entries.pool.recycle(&self.entries.slots, root);
        }
        match task.renew(&self.entries.slots) {
            ready::Renewal::Reusable(key) => ready::Reclaimed::Reusable(ready::Permit::new(key)),
            ready::Renewal::Retired(retired) => ready::Reclaimed::Retired(retired),
        }
    }

    pub(in crate::driver::schedule) fn drain<'a, 'd>(
        &'a self,
        access: ready::Access<'a, 'd>,
        budget: &mut lanes::Budget<'_, 'd, ready::Lane>,
        mut activate: impl FnMut(route::Erased<'d, kind::Any>),
    ) -> usize
    where
        'd: 'a,
    {
        let Some(mut ready) = self.ready.drain() else {
            return 0;
        };
        let mut drained = 0;
        while let quota::Admission::Item(index) = budget.admit(|| ready.next()) {
            drained += 1;
            match self.entries.slots.drain(index) {
                ready::Drain::Descriptor(slot) => {
                    if let Some(target) = self.descriptors.target_at(slot) {
                        activate(route::Erased::new(target, access.driver));
                    }
                }
                ready::Drain::Dispatch(target) => {
                    activate(route::Erased::new(target, access.driver));
                }
            }
        }
        drained
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    pub(super) fn task_parent<'a, 'd>(
        &'a self,
        access: ready::Access<'a, 'd>,
        key: ready::Key<'d>,
    ) -> ready::TaskParent<'d>
    where
        'd: 'a,
    {
        let Some(entry) = self.resolve(key.raw()) else {
            return ready::TaskParent::Stale;
        };
        match entry {
            ready::Entry::Task(task, _) => {
                ready::TaskParent::Task(access.bind(task.record().parent()))
            }
            ready::Entry::Dispatch(_) => ready::TaskParent::Root,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver) struct Stored<'a>(&'a Arena);

impl<'arena> Stored<'arena> {
    pub(in crate::driver::schedule::ready) fn activate(self, key: &ready::DispatchKey<'_>) {
        self.0.ready.insert(key.key().raw().dense());
    }

    pub(in crate::driver::schedule::ready) fn take(self, key: &ready::DispatchKey<'_>) -> Removal {
        self.0.ready.remove(key.key().raw().dense())
    }

    pub(in crate::driver) fn is_dispatch(self, proof: receive::Proof) -> bool {
        self.try_index(proof.key()).is_some()
    }

    pub(in crate::driver) fn credit_ready(self, proof: credit::Proof<'_>) {
        if let Some(index) = self.try_index(proof.key()) {
            self.0.ready.insert(index);
        }
    }

    pub(in crate::driver) fn try_receive(
        self,
        proof: receive::Proof,
    ) -> Result<(), receive::Proof> {
        let key = proof.key();
        let Some(index) = self.try_index(key) else {
            return Err(proof);
        };
        self.0.ready.insert(index);
        Ok(())
    }

    pub(in crate::driver) fn try_retained(
        self,
        key: ready::Retained<'_>,
    ) -> Result<(), ready::Stale> {
        let Some(index) = self.try_index(key.raw()) else {
            return Err(ready::Stale);
        };
        self.0.ready.insert(index);
        Ok(())
    }

    /// Activates a completion key that may name a dispatch or nested task.
    pub(in crate::driver::schedule::ready) fn complete<'a, 'd>(
        self,
        access: ready::Access<'a, 'd>,
        key: ready::Retained<'d>,
    ) where
        'arena: 'a,
        'd: 'a,
    {
        let Some(entry) = self.0.resolve(key.raw()) else {
            return;
        };
        self.0.queue_entry(access, entry);
    }

    fn try_index(self, key: ready::Stored) -> Option<ready::DynamicIndex> {
        match self.0.resolve(key)? {
            ready::Entry::Dispatch(index) => Some(index),
            ready::Entry::Task(_, _) => None,
        }
    }
}
