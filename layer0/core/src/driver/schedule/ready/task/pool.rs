use std::{marker, mem, num};

use dope_sys::table::permit;

use crate::{
    driver,
    driver::schedule::{ready, ready::task},
};

/// A driver-scoped lease of `N` task admissions without a stored parent.
#[doc(hidden)]
pub struct Pool<'d, Tag, const N: usize> {
    driver: driver::Reference<'d>,
    chain: ready::Chain,
    tag: marker::PhantomData<fn(Tag) -> Tag>,
}

/// One parent key proven to belong to a pool's driver and wake-hop ceiling.
#[doc(hidden)]
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Root<'d>(ready::Key<'d>);

/// Lifetime-free identity of one exact task root generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct RootId(u64);

impl<'d> Root<'d> {
    pub(super) fn initial(parent: task::Parent<'d>) -> Result<Self, task::Error> {
        parent.validate(task::MAX_WAKE_HOPS - 1)?;
        Ok(Self(parent.key()))
    }

    pub fn id(self) -> RootId {
        RootId(permit::raw::Key::identity(self.0.raw()))
    }
}

impl<'d, Tag, const N: usize> Pool<'d, Tag, N> {
    pub fn try_new(parent: task::Parent<'d>) -> Result<Self, task::Error> {
        let Some(count) = num::NonZero::<usize>::new(N) else {
            return Err(task::Error::Capacity {
                requested: num::NonZero::<usize>::MIN,
                available: 0,
            });
        };
        let driver = parent.driver();
        let entries = &driver.ready().arena().entries;
        let chain = entries.pool.try_take(&entries.slots, count)?;
        Ok(Self {
            driver,
            chain,
            tag: marker::PhantomData,
        })
    }

    pub fn try_root(&self, parent: task::Parent<'d>) -> Result<Root<'d>, task::Error> {
        Root::initial(parent)
    }

    pub fn parent(&self, root: Root<'d>) -> task::Parent<'d> {
        task::Parent(ready::Address::new(self.driver, root.0))
    }

    pub fn try_admit<'lease>(
        &'lease self,
        root: Root<'d>,
    ) -> Result<task::raw::Lease<'lease, 'd>, task::Error> {
        let entries = &self.driver.ready().arena().entries;
        let Some((child, chain)) = self.chain.try_claim(&entries.slots) else {
            return Err(task::Error::Capacity {
                requested: num::NonZero::<usize>::MIN,
                available: 0,
            });
        };
        Ok(task::raw::Lease::new(self.parent(root), child, chain))
    }

    pub(in crate::driver::schedule::ready::task) fn reclaim<R>(
        &self,
        root: Root<'d>,
        child: ready::Renewal<'d>,
        reusable: R,
        retired: R,
    ) -> R {
        match child {
            ready::Renewal::Reusable(child) => {
                self.parent(root).return_lease(child, &self.chain);
                reusable
            }
            ready::Renewal::Retired => retired,
        }
    }
}

impl<Tag, const N: usize> Drop for Pool<'_, Tag, N> {
    fn drop(&mut self) {
        let entries = &self.driver.ready().arena().entries;
        entries.pool.restore(&entries.slots, &self.chain);
    }
}

const _: () = assert!(mem::size_of::<Pool<'static, (), 0>>() == 3 * mem::size_of::<usize>());
const _: () =
    assert!(mem::size_of::<Pool<'static, (), 4096>>() == mem::size_of::<Pool<'static, (), 0>>());
const _: () = assert!(mem::size_of::<Root<'static>>() == mem::size_of::<usize>());
