use std::marker;

use o3::depot::capacity;
use schedule::{Plane, queue};

use crate::{
    core::driver::schedule::ready::task::owner,
    task::storage::fixed::{quota, schedule, tree},
};

/// Allocation-free description of a bounded schedule task plane.
pub struct Plan<'d, R>
where
    R: tree::Tree<'d>,
{
    limit: quota::Limit,
    brand: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    tree: marker::PhantomData<fn() -> R>,
}

impl<'d, R> Plan<'d, R>
where
    R: tree::Tree<'d>,
{
    pub fn new(limit: quota::Limit) -> Self {
        Self {
            limit,
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

    /// Caps each route's physical slots independently of the global admission limit.
    /// Retired slots are not replaced from the unallocated declared suffix.
    pub fn try_prepare_with_capacity<Source>(
        self,
        owners: owner::Capacity,
        physical: capacity::Count,
    ) -> Result<Plane<'d, R, quota::Bounded, queue::Owned<'d, Source>>, queue::PrepareError> {
        let tasks = R::TASKS;
        let routes = R::try_build_with_capacity(physical).map_err(queue::PrepareError::Routes)?;
        let queue = queue::Owned::try_new(tasks, owners)?;
        let quota = quota::Bounded::new(self.limit, tasks);
        Ok(Plane::from_parts(routes, quota, queue))
    }
}
