use core::{mem, pin};

use o3::{alloc, alloc::tail, depot::capacity};
use slot::state::State;

use crate::{
    abi,
    context::wake,
    core::driver::{retained, schedule::app},
    task::storage::{
        self, Id, phase,
        scope::{Domain, Scope, Scoped},
        set,
    },
};

mod entries;
pub mod group;
pub mod insert;
pub(in crate::task::storage) mod pools;
mod ready;
mod reserves;
pub(in crate::task::storage) mod slot;
pub(in crate::task::storage) mod views;

type Allocation<'d, F, M, D, G> =
    tail::Block<pools::Header<'d, F, M, D, G>, slot::Slot<'d, F, M, D, G>, capacity::Count>;

type Borrowed<'a, 'd, F, M, D, G> =
    tail::View<'a, pools::Header<'d, F, M, D, G>, slot::Slot<'d, F, M, D, G>, capacity::Count>;

/// A bounded count of fibers in one task-set operation.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Length(u32);

impl Length {
    pub const ZERO: Self = Self(0);

    pub(in crate::task::storage::set) const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0 as usize
    }

    pub(in crate::task::storage::set) const fn raw(self) -> u32 {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Length>() == mem::size_of::<u32>());

/// Whether a task set has a completion ready to poll.
#[must_use = "task-set work must be consumed"]
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Work {
    Idle,
    Pending,
}

const _: () = assert!(mem::size_of::<Work>() == mem::size_of::<u8>());

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum WakeError {
    Missing,
    Inactive,
}

/// A fixed-capacity set of pinned fibers and their retained completion state.
pub struct Set<'d, F, M, D, G = ()>
where
    F: abi::Fiber<'d>,
{
    pool: pools::State<'d, F, M, D, G>,
}

impl<'d, F, M, D, G> Set<'d, F, M, D, G>
where
    F: abi::Fiber<'d>,
{
    pub fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            pool: pools::State::try_new(capacity)?,
        })
    }

    pub(crate) fn try_insert(
        &mut self,
        fiber: F,
        data: M,
        parent: wake::RootWaker<'d>,
    ) -> Result<Id<'d, D>, insert::Error> {
        let mut entries = self.pool.entries();
        let entry = entries.vacant().ok_or(insert::Error::Full)?;
        entry.try_insert(fiber, data, parent)
    }

    pub(crate) fn access<R>(
        &mut self,
        id: &Id<'d, D>,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R> {
        let mut entries = self.pool.entries();
        let owner = storage::Owner::of(entries.view().header());
        let key = id.key_for(owner)?;
        entries.resident(key, access)
    }

    pub(crate) fn read<R>(
        &self,
        id: &Id<'d, D>,
        read: impl for<'m> FnOnce(&'m M) -> R,
    ) -> Option<R> {
        let view = self.pool.view();
        let owner = storage::Owner::of(view.header());
        let key = id.key_for(owner)?;
        view.read(key, read)
    }

    pub(crate) fn inspect<R>(
        &self,
        id: &Id<'d, D>,
        inspect: impl for<'m> FnOnce(&'m M, phase::State) -> R,
    ) -> Option<R> {
        let view = self.pool.view();
        let owner = storage::Owner::of(view.header());
        let key = id.key_for(owner)?;
        view.inspect(key, inspect)
    }

    pub fn poll_ready(
        &mut self,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Length {
        if ready::Queue::is_empty(&self.pool.view()) {
            return Length::ZERO;
        }
        ready::Queue::drain(self.pool.entries(), work.remaining().saturating_add(1))
            .drive(work, driver)
    }

    pub fn take_done(&mut self) -> Option<(M, F::Output)> {
        let entries = self.pool.entries();
        let (view, mut token) = entries.into_parts();
        let done = ready::Queue::pop(view.reborrow(), &mut token)?;
        Some(done.take())
    }

    pub(crate) fn try_wake(&self, id: &Id<'d, D>) -> Result<(), WakeError> {
        let view = self.pool.view();
        let owner = storage::Owner::of(view.header());
        let key = id.key_for(owner).ok_or(WakeError::Missing)?;
        view.try_wake(key)
    }

    pub fn wake_next(&mut self) {
        let mut ready = ready::Queue::drain(self.pool.entries(), 1);
        let Some(queued) = ready.peek() else {
            return;
        };
        queued.wake();
    }

    pub fn work(&self) -> Work {
        if ready::Queue::is_empty(&self.pool.view()) {
            Work::Idle
        } else {
            Work::Pending
        }
    }

    pub fn len(&self) -> Length {
        self.pool.len()
    }

    pub(crate) fn remove(&mut self, id: Id<'d, D>) -> Result<(), Id<'d, D>> {
        let state = {
            let mut entries = self.pool.entries();
            let owner = storage::Owner::of(entries.view().header());
            let Some(key) = id.key_for(owner) else {
                return Err(id);
            };
            let Some(state) = entries.remove(key) else {
                return Err(id);
            };
            state
        };
        if state == State::Queued {
            self.wake_next();
        }
        Ok(())
    }
}

/// Operations on dynamic task storage retained by an identity scope.
pub trait Access<'d, F: abi::Fiber<'d>, M, D> {
    fn try_insert(
        &mut self,
        fiber: F,
        data: M,
        parent: wake::RootWaker<'d>,
    ) -> Result<Id<'d, D>, insert::Error>;
    fn access<R>(
        &mut self,
        id: &Id<'d, D>,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R>;
    fn read<R>(&self, id: &Id<'d, D>, read: impl for<'m> FnOnce(&'m M) -> R) -> Option<R>;
    fn inspect<R>(
        &self,
        id: &Id<'d, D>,
        inspect: impl for<'m> FnOnce(&'m M, phase::State) -> R,
    ) -> Option<R>;
    fn try_wake(&self, id: &Id<'d, D>) -> Result<(), set::WakeError>;
    fn poll_ready(
        &mut self,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> set::Length;
    fn take_done(&mut self) -> Option<(M, F::Output)>;
    fn wake_next(&mut self);
    fn remove(&mut self, id: Id<'d, D>) -> Result<(), Id<'d, D>>;
}

impl<'s, 'd, F: abi::Fiber<'d>, M, D, G> Access<'d, F, M, Domain<'s, D>>
    for Scope<'s, pin::Pin<&mut set::Set<'d, F, M, D, G>>>
{
    fn try_insert(
        &mut self,
        fiber: F,
        data: M,
        parent: wake::RootWaker<'d>,
    ) -> Result<Scoped<'s, 'd, D>, insert::Error> {
        self.source
            .as_mut()
            .get_mut()
            .try_insert(fiber, data, parent)
            .map(|id| id.bind(self.proof))
    }

    fn access<R>(
        &mut self,
        id: &Scoped<'s, 'd, D>,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R> {
        self.source
            .as_mut()
            .get_mut()
            .access(&id.original(), access)
    }

    fn read<R>(&self, id: &Scoped<'s, 'd, D>, read: impl for<'m> FnOnce(&'m M) -> R) -> Option<R> {
        self.source.read(&id.original(), read)
    }

    fn inspect<R>(
        &self,
        id: &Scoped<'s, 'd, D>,
        inspect: impl for<'m> FnOnce(&'m M, phase::State) -> R,
    ) -> Option<R> {
        self.source.inspect(&id.original(), inspect)
    }

    fn try_wake(&self, id: &Scoped<'s, 'd, D>) -> Result<(), set::WakeError> {
        self.source.try_wake(&id.original())
    }

    fn poll_ready(
        &mut self,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> set::Length {
        self.source.as_mut().get_mut().poll_ready(work, driver)
    }

    fn take_done(&mut self) -> Option<(M, F::Output)> {
        self.source.as_mut().get_mut().take_done()
    }

    fn wake_next(&mut self) {
        self.source.as_mut().get_mut().wake_next();
    }

    fn remove(&mut self, id: Scoped<'s, 'd, D>) -> Result<(), Scoped<'s, 'd, D>> {
        self.source
            .as_mut()
            .get_mut()
            .remove(id.original())
            .map_err(|id| id.bind(self.proof))
    }
}
