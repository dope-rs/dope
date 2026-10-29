use std::{marker, mem};

use o3::base::marker::ThreadBound;
use vacancy::Vacancy;

use crate::driver::schedule::clock::timer::heap::{Lease, Minimum, State, core, order};

pub(in crate::driver::schedule::clock::timer::heap) mod vacancy;

/// Branded operational view over one persistent leased heap store.
pub struct Min<'a, 'id, P: Ord, R, Tag = ()> {
    core: &'a mut core::Core<P, R>,
    brand: marker::PhantomData<fn(&'id Tag) -> &'id Tag>,
    thread: marker::PhantomData<ThreadBound>,
}

impl<'a, 'id, P: Ord, R, Tag> Min<'a, 'id, P, R, Tag> {
    pub(in crate::driver::schedule::clock::timer::heap) fn new(
        core: &'a mut core::Core<P, R>,
        brand: marker::PhantomData<fn(&'id Tag) -> &'id Tag>,
    ) -> Self {
        Self {
            core,
            brand,
            thread: marker::PhantomData,
        }
    }

    /// Returns exclusive proof that one resident slot is available.
    pub fn vacancy(&mut self) -> Option<Vacancy<'_, 'a, 'id, P, R, Tag>> {
        let free = self.core.free()?;
        Some(Vacancy { heap: self, free })
    }

    /// Releases one resident and any armed priority in constant or logarithmic time.
    pub fn release(&mut self, lease: Lease<'id, Tag>) -> R {
        self.core.release(lease.live())
    }

    pub fn state(&self, lease: &Lease<'id, Tag>) -> State {
        self.core.state(lease.live())
    }

    pub fn priority(&self, lease: &Lease<'id, Tag>) -> Option<&P> {
        self.core.priority(lease.live())
    }

    pub fn resident(&self, lease: &Lease<'id, Tag>) -> &R {
        self.core.resident(lease.live())
    }

    /// Arms or reprioritizes while destroying the replaced priority.
    pub fn set(&mut self, lease: &Lease<'id, Tag>, prio: P) {
        self.core.set(lease.live(), prio);
    }

    /// Disarms while destroying the removed priority.
    pub fn clear(&mut self, lease: &Lease<'id, Tag>) {
        self.core.clear(lease.live());
    }

    pub fn minimum(&mut self) -> Option<Minimum<'_, 'id, P, R, Tag>> {
        let root = order::Order::new(self.core).root()?;
        Some(Minimum::new(&mut *self.core, self.brand, root))
    }

    pub fn peek(&self) -> Option<&P> {
        order::Order::new(self.core).peek()
    }
}

const _: () = {
    assert!(mem::size_of::<Min<'_, '_, u8, u8>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Min<'_, '_, u8, u8>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Min<'_, '_, u8, u8>>>() == mem::size_of::<usize>());
};
