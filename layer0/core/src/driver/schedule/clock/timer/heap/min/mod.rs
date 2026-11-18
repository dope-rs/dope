use std::{marker, mem, task, time};

use o3::base::marker::ThreadBound;
use vacancy::Vacancy;

use crate::driver::schedule::{
    clock::timer::{
        self,
        heap::{IDLE, Lease, Minimum, State, core, order},
    },
    ready::wake,
};

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
        self.remove(lease).0
    }

    pub(in crate::driver::schedule::clock::timer) fn remove(
        &mut self,
        lease: Lease<'id, Tag>,
    ) -> (R, State<P>) {
        self.core.release(lease.slot())
    }

    pub fn state(&self, lease: &Lease<'id, Tag>) -> State<&P> {
        self.core.state(lease.slot())
    }

    pub fn priority(&self, lease: &Lease<'id, Tag>) -> Option<&P> {
        self.core.priority(lease.slot())
    }

    pub fn resident(&self, lease: &Lease<'id, Tag>) -> &R {
        self.core.resident(lease.slot())
    }

    /// Arms or reprioritizes while destroying the replaced priority.
    pub fn set(&mut self, lease: &Lease<'id, Tag>, prio: P) {
        self.core.set(lease.slot(), prio);
    }

    /// Disarms while destroying the removed priority.
    pub fn clear(&mut self, lease: &Lease<'id, Tag>) {
        self.core.clear(lease.slot());
    }

    pub fn minimum(&mut self) -> Option<Minimum<'_, 'id, P, R, Tag>> {
        let root = self.core.root()?;
        Some(Minimum::new(&mut *self.core, self.brand, root))
    }

    pub fn peek(&self) -> Option<&P> {
        self.core.peek()
    }
}

impl<'d> timer::Command<'_, 'd> {
    pub(in crate::driver::schedule::clock::timer) fn poll_alarm(
        &mut self,
        lease: &Lease<'d, timer::Tag>,
        now: time::Instant,
        target: &wake::Waker<'d>,
        start: impl FnOnce() -> Option<time::Instant>,
    ) -> task::Poll<()> {
        let slot = lease.slot();
        match self.core.phase(slot) {
            State::Idle => {
                let Some(due) = start() else {
                    return task::Poll::Pending;
                };
                if due <= now {
                    return task::Poll::Ready(());
                }
                order::insert(self.core, slot, due);
            }
            State::Armed(position) if *self.core.priority_at(position) > now => {}
            phase @ (State::Armed(_) | State::Fired) => {
                if let State::Armed(position) = phase {
                    self.core.remove_at(position);
                    self.core.resident(slot).clear();
                }
                self.core.write_state(slot, IDLE);
                return task::Poll::Ready(());
            }
        }
        self.core.resident(slot).set(*target.key());
        task::Poll::Pending
    }
}

const _: () = {
    assert!(mem::size_of::<Min<'_, '_, u8, u8>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Min<'_, '_, u8, u8>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Min<'_, '_, u8, u8>>>() == mem::size_of::<usize>());
};
