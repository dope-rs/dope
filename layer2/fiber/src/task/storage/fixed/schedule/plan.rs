use std::marker;

use o3::depot::capacity;
use schedule::{Plane, plane::Active, queue};

use crate::{
    core::driver::schedule::ready::task::owner,
    task::storage::fixed::{quota, schedule, tree},
};

/// Allocation-free description of a bounded schedule task plane.
pub struct Plan<'d, R>
where
    R: tree::Tree<'d>,
{
    quota: quota::Bounded,
    brand: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    tree: marker::PhantomData<fn() -> R>,
}

impl<'d, R> Plan<'d, R>
where
    R: tree::Tree<'d>,
{
    pub fn new(limit: quota::Limit) -> Self {
        Self {
            quota: quota::Bounded::new(limit, R::TASKS),
            brand: marker::PhantomData,
            tree: marker::PhantomData,
        }
    }
}

impl<'d, R> Plan<'d, R>
where
    R: schedule::PlaneTree<'d>,
{
    pub fn try_prepare<Source>(
        self,
        owners: owner::Capacity,
    ) -> Result<Plane<'d, R, quota::Bounded, queue::Owned<'d, Source>>, queue::PrepareError> {
        self.try_prepare_with_capacity(owners, capacity::Count::MAX)
    }

    /// Caps each route's physical slots within the global admission limit.
    pub fn try_prepare_with_capacity<Source>(
        self,
        owners: owner::Capacity,
        physical: capacity::Count,
    ) -> Result<Plane<'d, R, quota::Bounded, queue::Owned<'d, Source>>, queue::PrepareError> {
        let physical = physical.min(capacity::Count::new(self.quota.available().get() as u32));
        let routes = R::try_build_with_capacity(physical).map_err(queue::PrepareError::Routes)?;
        let (active, tasks) = Active::new(routes, self.quota);
        let queue = queue::Owned::try_new(tasks, owners)?;
        Ok(Plane::from_parts(active, queue))
    }
}
