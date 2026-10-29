use std::mem;

use crate::driver::schedule::{
    ready,
    ready::{
        task,
        task::pool::{Pool, Root},
    },
};

/// Parent-bound task admissions.
///
pub struct Domain<'d, Tag, const N: usize> {
    pool: Pool<'d, Tag, N>,
    root: Root<'d>,
}

impl<'d, Tag, const N: usize> Domain<'d, Tag, N> {
    pub fn try_new(parent: task::Parent<'d>) -> Result<Self, task::Error> {
        let root = Root::initial(parent)?;
        let pool = Pool::try_new(parent)?;
        Ok(Self { pool, root })
    }

    /// Tries to admit one task from this domain's exact reserved capacity.
    pub fn try_admit<'lease>(
        &'lease mut self,
    ) -> Result<task::raw::Lease<'lease, 'd>, task::Error> {
        self.pool.try_admit(self.root)
    }

    pub(in crate::driver::schedule::ready::task) fn parent(&self) -> task::Parent<'d> {
        self.pool.parent(self.root)
    }

    pub(in crate::driver::schedule::ready::task) fn reclaim(
        &mut self,
        hold: ready::Reclaimed<'d>,
    ) -> task::Reclaim {
        self.pool.reclaim(
            self.root,
            hold,
            task::Reclaim::Reusable,
            task::Reclaim::Retired,
        )
    }

    pub fn wake_parent(&self) {
        self.parent().wake();
    }
}

const _: () = assert!(mem::size_of::<Domain<'static, (), 0>>() == 3 * mem::size_of::<usize>());
const _: () = assert!(
    mem::size_of::<Domain<'static, (), 4096>>() == mem::size_of::<Domain<'static, (), 0>>()
);
