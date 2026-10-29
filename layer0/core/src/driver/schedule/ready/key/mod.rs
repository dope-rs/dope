use std::{mem, num};

use o3::mem::Return;

use crate::{
    driver,
    driver::{
        route::indices,
        schedule::{ready, ready::state},
    },
    io::fd::{handles, identity},
};

pub(in crate::driver::schedule::ready) mod retained;
mod stored;

use retained::Retained;
pub(crate) use stored::Stored;

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct ReservedKey<'d>(ready::Key<'d>);

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct DispatchKey<'d>(ready::Key<'d>);

#[repr(transparent)]
pub(super) struct Staged<'d>(ready::Key<'d>);

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct TaskKey<'d>(ready::Key<'d>);

pub(in crate::driver::schedule::ready) struct Detached<'d> {
    pub(in crate::driver::schedule::ready) task: ReservedKey<'d>,
    pub(in crate::driver::schedule::ready) root: Option<ReservedKey<'d>>,
}

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct TopologyKey<'d>(ready::Key<'d>);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct FreeIndex(num::NonZero<u16>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct TaskIndex(num::NonZero<u16>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct RootIndex(num::NonZero<u16>);

/// Absolute index in the validated shared ready arena.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct DynamicIndex(pub(in crate::driver::schedule::ready) num::NonZero<u16>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Local(pub(super) num::NonZero<u16>);

pub(super) trait Index {
    type State;

    fn code(self) -> num::NonZero<u16>;
}

pub(crate) trait QueueIndex {
    fn queue_index(&self) -> DynamicIndex;
}

impl Index for FreeIndex {
    type State = state::Free;

    fn code(self) -> num::NonZero<u16> {
        self.0
    }
}

impl Index for RootIndex {
    type State = state::Topology;

    fn code(self) -> num::NonZero<u16> {
        self.0
    }
}

impl Index for TaskIndex {
    type State = state::Task;

    fn code(self) -> num::NonZero<u16> {
        self.0
    }
}

impl Index for Local {
    type State = state::Any;

    fn code(self) -> num::NonZero<u16> {
        self.0
    }
}

impl Index for &DispatchKey<'_> {
    type State = state::Dispatch;

    fn code(self) -> num::NonZero<u16> {
        self.0.raw().index_code()
    }
}

impl Index for &ReservedKey<'_> {
    type State = state::Reserved;

    fn code(self) -> num::NonZero<u16> {
        self.0.raw().index_code()
    }
}

impl Index for &TaskKey<'_> {
    type State = state::Task;

    fn code(self) -> num::NonZero<u16> {
        self.0.raw().index_code()
    }
}

impl Index for &TopologyKey<'_> {
    type State = state::Topology;

    fn code(self) -> num::NonZero<u16> {
        self.0.raw().index_code()
    }
}

impl DynamicIndex {
    pub(in crate::driver) const fn get(self) -> usize {
        (self.0.get() - 1) as usize
    }

    pub(in crate::driver) fn raw(self) -> u32 {
        u32::from(self.0.get() - 1)
    }

    pub(in crate::driver) const fn from_code(code: num::NonZero<u16>) -> Self {
        Self(code)
    }
}

fn descriptor(index: indices::SlotIndex) -> DynamicIndex {
    // SAFETY: QueueIndex is implemented only for capabilities issued by the
    // descriptor domain bounded by Plan's u16 ready domain.
    DynamicIndex(unsafe { num::NonZero::<u16>::new_unchecked(index.raw() as u16 + 1) })
}

impl QueueIndex for identity::Identity<'_> {
    fn queue_index(&self) -> DynamicIndex {
        descriptor((*self).index())
    }
}

impl QueueIndex for identity::Identity<'_, handles::OutboundSlot> {
    fn queue_index(&self) -> DynamicIndex {
        descriptor((*self).slot().physical().into())
    }
}

impl QueueIndex for handles::Accepted {
    fn queue_index(&self) -> DynamicIndex {
        descriptor(self.physical().into())
    }
}

impl QueueIndex for handles::OutboundSlot {
    fn queue_index(&self) -> DynamicIndex {
        descriptor((*self).physical().into())
    }
}

impl<'d> ReservedKey<'d> {
    pub(super) fn new(index: ready::FreeIndex, epoch: ready::Generation) -> Self {
        Self(ready::Key::from_index(index.0, epoch))
    }

    pub(super) fn key(&self) -> ready::Key<'d> {
        self.0
    }

    pub(super) fn dispatch(self) -> DispatchKey<'d> {
        DispatchKey(self.0)
    }

    pub(super) fn stage(self) -> Staged<'d> {
        Staged(self.0)
    }

    pub(super) fn topology(self) -> TopologyKey<'d> {
        TopologyKey(self.0)
    }

    pub(in crate::driver::schedule::ready) fn renew(
        self,
        slots: &'d ready::Slots,
    ) -> ready::Renewal<'d> {
        slots.mutation().renew(self)
    }
}

impl<'d> DispatchKey<'d> {
    pub(in crate::driver::schedule::ready) fn key(&self) -> ready::Key<'d> {
        self.0
    }

    pub(super) fn reserve(self) -> ReservedKey<'d> {
        ReservedKey(self.0)
    }
}

impl<'d> Return for DispatchKey<'d> {
    type Item = driver::Reference<'d>;

    fn return_item(self, driver: Self::Item) {
        driver.ready().arena().free_route(self);
    }
}

impl<'d> Return for TopologyKey<'d> {
    type Item = driver::Reference<'d>;

    fn return_item(self, driver: Self::Item) {
        let arena = driver.ready().arena();
        arena.entries.pool.release_root(&arena.entries.slots, self);
    }
}

impl<'d> Staged<'d> {
    pub(super) fn attach(self) -> TaskKey<'d> {
        TaskKey(self.0)
    }
}

impl<'d> TaskKey<'d> {
    pub(in crate::driver::schedule::ready) fn key(&self) -> ready::Key<'d> {
        self.0
    }

    pub(super) fn reserve(self) -> ReservedKey<'d> {
        ReservedKey(self.0)
    }

    pub(in crate::driver::schedule::ready) fn detach(
        self,
        slots: &'d ready::Slots,
    ) -> Detached<'d> {
        let root = ready::Topology::detach(slots, &self);
        let task = slots.mutation().untask(self);
        Detached { task, root }
    }
}

impl<'d> TopologyKey<'d> {
    pub(super) fn new(index: ready::FreeIndex, epoch: ready::Generation) -> Self {
        Self(ready::Key::from_index(index.0, epoch))
    }

    pub(in crate::driver::schedule::ready) fn key(&self) -> ready::Key<'d> {
        self.0
    }

    pub(super) fn reserve(self) -> ReservedKey<'d> {
        ReservedKey(self.0)
    }
}

impl FreeIndex {
    pub(super) const fn from_code(code: num::NonZero<u16>) -> Self {
        Self(code)
    }

    pub(super) fn from_key(key: ready::Key<'_>) -> Self {
        Self(key.raw().index_code())
    }
}

impl TaskIndex {
    pub(super) fn new(key: &TaskKey<'_>) -> Self {
        Self(key.key().raw().index_code())
    }

    pub(in crate::driver::schedule::ready) const fn from_code(code: num::NonZero<u16>) -> Self {
        Self(code)
    }
}

impl RootIndex {
    pub(super) fn new(key: &TopologyKey<'_>) -> Self {
        Self(key.key().raw().index_code())
    }

    pub(super) const fn from_code(code: num::NonZero<u16>) -> Self {
        Self(code)
    }
}

const _: () = {
    assert!(mem::size_of::<Stored>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Retained<'static>>() == mem::size_of::<Stored>());
    assert!(mem::align_of::<Retained<'static>>() == mem::align_of::<Stored>());
    assert!(mem::size_of::<ReservedKey<'static>>() == mem::size_of::<Stored>());
    assert!(mem::size_of::<DispatchKey<'static>>() == mem::size_of::<Stored>());
    assert!(mem::size_of::<Staged<'static>>() == mem::size_of::<Stored>());
    assert!(mem::size_of::<TaskKey<'static>>() == mem::size_of::<Stored>());
    assert!(mem::size_of::<TopologyKey<'static>>() == mem::size_of::<Stored>());
    assert!(mem::size_of::<Option<DispatchKey<'static>>>() == mem::size_of::<Stored>());
    assert!(mem::size_of::<Option<TopologyKey<'static>>>() == mem::size_of::<Stored>());
    assert!(mem::size_of::<FreeIndex>() == mem::size_of::<u16>());
    assert!(mem::size_of::<TaskIndex>() == mem::size_of::<u16>());
    assert!(mem::size_of::<RootIndex>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Local>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Option<TaskIndex>>() == mem::size_of::<u16>());
};
