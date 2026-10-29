use std::{marker, mem, num};

pub(crate) use arena::Arena;
use arena::{
    cells::{Drain, Entry, Renewal, Resident, Slots, Table},
    layout, payload, pool, topology,
};
pub use key::retained::Retained;
use key::{
    Detached, DispatchKey, FreeIndex, Index, ReservedKey, RootIndex, Staged, TaskIndex, TaskKey,
    TopologyKey,
};
pub(crate) use key::{DynamicIndex, QueueIndex, Stored};
use kind::Kind;
use layout::Layout;
use o3::{
    base::{marker::ThreadBound, num::Generation},
    depot::queue::Removal,
    mem::Lease,
};
use payload::{Link, Task};
use pool::{Chain, Permit, Pool, Reclaimed};
use record::Record;
pub use rounds::Rounds;
use topology::Topology;

use crate::{driver, driver::route, io::fd::identity};

mod arena;
mod record;
mod rounds;

pub(super) enum Lane {}

/// Proof that a retained ready generation no longer occupies its slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stale;

/// Readiness observed while taking one exact live slot.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Readiness {
    Idle,
    Ready,
}

/// Dynamic ready capacity was exhausted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Capacity {
    available: u32,
}

impl Capacity {
    const fn new(free: u32) -> Self {
        Self { available: free }
    }

    #[must_use]
    pub const fn available(self) -> usize {
        self.available as usize
    }
}

#[derive(Clone, Copy)]
pub(in crate::driver) struct Access<'a, 'd> {
    borrow: marker::PhantomData<fn(&'a ()) -> &'a ()>,
    driver: driver::Invariant<'d>,
}

impl<'d> Access<'_, 'd> {
    pub(in crate::driver) fn new<'a>(driver: &'a driver::Reference<'d>) -> Access<'a, 'd>
    where
        'd: 'a,
    {
        Access {
            borrow: marker::PhantomData,
            driver: driver.brand,
        }
    }

    fn bind(self, stored: Stored) -> Key<'d> {
        Key {
            stored,
            arena: self.driver,
            thread: ThreadBound::NEW,
        }
    }
}

enum TaskParent<'d> {
    Root,
    Task(Key<'d>),
    Stale,
}

#[doc(hidden)]
pub mod task;
pub mod wake;

/// A generation-checked address in a driver's local ready queue.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Key<'d> {
    stored: Stored,
    arena: driver::Invariant<'d>,
    thread: ThreadBound,
}

impl PartialEq for Key<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.stored == other.stored
    }
}

impl Eq for Key<'_> {}

impl<'d> Key<'d> {
    fn from_index(index: num::NonZero<u16>, epoch: Generation) -> Self {
        Self {
            stored: Stored::from_parts(index, epoch),
            arena: driver::Invariant(marker::PhantomData),
            thread: ThreadBound::NEW,
        }
    }

    pub(in crate::driver) const fn raw(self) -> Stored {
        self.stored
    }
}

#[derive(Clone, Copy)]
struct Address<'d> {
    driver: driver::Reference<'d>,
    key: Retained<'d>,
}

impl PartialEq for Address<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.driver.is_same(other.driver) && self.key == other.key
    }
}

impl Eq for Address<'_> {}

impl<'d> Address<'d> {
    fn new(driver: driver::Reference<'d>, key: Key<'d>) -> Self {
        Self {
            driver,
            key: Retained(key),
        }
    }

    fn wake(self) {
        self.driver.ready().activate(self.key.0);
    }

    fn belongs_to(self, driver: driver::Reference<'_>) -> bool {
        self.driver.is_same(driver)
    }

    fn key(self) -> Key<'d> {
        self.key.0
    }

    fn retarget(self, key: Key<'d>) -> Self {
        Self::new(self.driver, key)
    }
}

/// A generation-checked driver readiness target that cannot name a task node.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Target<'d> {
    address: Address<'d>,
}

impl PartialEq for Target<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl Eq for Target<'_> {}

impl<'d> Target<'d> {
    pub(crate) fn new(driver: driver::Reference<'d>, key: Key<'d>) -> Self {
        Self {
            address: Address::new(driver, key),
        }
    }

    pub fn wake(self) {
        self.address.wake();
    }

    #[doc(hidden)]
    pub fn key(&self) -> Retained<'d> {
        self.address.key
    }

    pub(in crate::driver) fn receive(self) -> (driver::Reference<'d>, Stored) {
        (self.address.driver, self.address.key.raw())
    }

    fn address(self) -> Address<'d> {
        self.address
    }
}

#[derive(Clone, Copy)]
pub struct Handle<'d> {
    driver: driver::Reference<'d>,
    id: identity::Identity<'d>,
}

impl<'d> Handle<'d> {
    pub(in crate::driver) const fn new(
        driver: driver::Reference<'d>,
        id: identity::Identity<'d>,
    ) -> Self {
        Self { driver, id }
    }

    pub fn set_target<Tag: route::Tag>(self, target: route::Operation<'d, Tag>) {
        self.driver
            .descriptors()
            .set_target(self.id, target.into_token());
    }

    pub fn activate(self) {
        if self.driver.descriptors().target(self.id).is_some() {
            self.driver.ready().arena().activate_fd(self.id);
        }
    }

    pub fn key(self) -> Key<'d> {
        let index = self.id.queue_index();
        Key::from_index(index.0, self.id.generation())
    }

    pub fn target(self) -> Target<'d> {
        Target::new(self.driver, self.key())
    }
}

/// One ready slot branded by both driver domain and route tag.
///
pub struct Slot<'d, Tag: route::Tag> {
    owner: Lease<DispatchKey<'d>>,
    target: marker::PhantomData<*mut Tag>,
}

impl<'d, Tag: route::Tag> Slot<'d, Tag> {
    fn new(driver: driver::Reference<'d>, key: DispatchKey<'d>) -> Self {
        Self {
            owner: Lease::new(key, driver),
            target: marker::PhantomData,
        }
    }

    pub fn activate(&self) {
        self.owner
            .item()
            .ready()
            .arena()
            .stored()
            .activate(self.dispatch_key());
    }

    /// Takes readiness for this exact live slot without draining other entries.
    #[must_use]
    pub fn take(&self) -> Readiness {
        match self
            .owner
            .item()
            .ready()
            .arena()
            .stored()
            .take(self.dispatch_key())
        {
            Removal::Removed => Readiness::Ready,
            Removal::Absent => Readiness::Idle,
        }
    }

    /// Returns the driver- and generation-branded root owned by this slot.
    pub fn target(&self) -> Target<'d> {
        Target::new(*self.owner.item(), self.dispatch_key().key())
    }

    fn dispatch_key(&self) -> &DispatchKey<'d> {
        self.owner.sink()
    }
}

const _: () = {
    assert!(mem::size_of::<Readiness>() == mem::size_of::<u8>());
    assert!(mem::size_of::<usize>() >= mem::size_of::<u32>());
    assert!(mem::size_of::<Access<'static, 'static>>() == 0);
};
const _: () = {
    assert!(mem::size_of::<Key<'static>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Key<'static>>>() == mem::size_of::<Key<'static>>());
    assert!(mem::size_of::<Option<Retained<'static>>>() == mem::size_of::<Retained<'static>>());
    assert!(mem::size_of::<DispatchKey<'static>>() == mem::size_of::<Key<'static>>());
    assert!(mem::align_of::<DispatchKey<'static>>() == mem::align_of::<Key<'static>>());
    assert!(mem::size_of::<Slot<'static, route::KeyTag<0>>>() == 2 * mem::size_of::<usize>());
};
const _: () = assert!(mem::size_of::<Target<'static>>() == 2 * mem::size_of::<usize>());
const _: () = {
    assert!(mem::size_of::<Handle<'static>>() == 2 * mem::size_of::<usize>());
};

mod key;
mod kind;
mod state;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Context<'d>(driver::Reference<'d>);

impl<'d> Context<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(crate) fn arena(self) -> &'d Arena {
        self.0.shared.context().schedule.owner.context()
    }

    pub(crate) fn handle(self, id: identity::Identity<'d>) -> Handle<'d> {
        Handle::new(self.0, id)
    }

    pub fn make_slot<Tag: route::Tag>(
        self,
        target: route::Operation<'d, Tag>,
    ) -> Result<Slot<'d, Tag>, Capacity> {
        self.arena().make_slot(self.0, target)
    }

    /// Activates a key branded by this exact driver domain.
    ///
    pub fn activate(self, key: Key<'d>) {
        self.arena().activate(Access::new(&self.0), key);
    }

    /// Activates a retained key when its generation is still current.
    pub fn try_activate(self, key: Retained<'d>) -> Result<(), Stale> {
        self.arena().stored().try_retained(key)
    }

    pub fn has_ready(self) -> bool {
        self.arena().has_ready()
    }
}

const _: () = assert!(
    std::mem::size_of::<Context<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);
