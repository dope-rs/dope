use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::Poll;

use dope::panic::abort_on_drop_panic;
use o3::collections::{FixedPinSlab, FixedPinSlabVacantEntry, PinSlab, SlabKey, SlabKeyParts};
use pin_project::{pin_project, pinned_drop};

use crate::{Context, Fiber};

pub struct TaskId<Tag = ()> {
    parts: SlabKeyParts,
    marker: PhantomData<*mut Tag>,
}

impl<Tag> TaskId<Tag> {
    fn from_key(key: SlabKey<Tag>) -> Self {
        Self {
            parts: key.parts(),
            marker: PhantomData,
        }
    }

    fn parts(&self) -> SlabKeyParts {
        self.parts
    }

    pub fn index(&self) -> usize {
        self.parts.index() as usize
    }

    pub fn erase(self) -> ErasedTaskId {
        ErasedTaskId {
            parts: self.parts,
            marker: PhantomData,
        }
    }

    pub fn from_erased(id: ErasedTaskId) -> Self {
        Self {
            parts: id.parts,
            marker: PhantomData,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErasedTaskId {
    parts: SlabKeyParts,
    marker: PhantomData<*mut ()>,
}

impl ErasedTaskId {
    pub fn index(self) -> usize {
        self.parts.index() as usize
    }
}

fn remove_catching_drop_panic(operation: impl FnOnce() -> bool) -> bool {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(removed) => removed,
        Err(payload) => {
            abort_on_drop_panic(payload);
            true
        }
    }
}

pub struct Slab<'d, F, Tag = ()>
where
    F: Fiber<'d>,
{
    inner: PinSlab<F, Tag>,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F, Tag> Slab<'d, F, Tag>
where
    F: Fiber<'d>,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: PinSlab::with_capacity(capacity),
            driver: PhantomData,
        }
    }

    pub fn insert(&mut self, fiber: F) -> Option<TaskId<Tag>> {
        self.inner.insert(fiber).ok().map(TaskId::from_key)
    }

    pub(crate) fn contains(&self, id: &TaskId<Tag>) -> bool {
        self.inner.contains_parts(id.parts())
    }

    pub fn poll(
        &mut self,
        id: &TaskId<Tag>,
        context: Pin<&mut Context<'_, 'd>>,
    ) -> Option<Poll<F::Output>> {
        let fiber = self.inner.get_parts_mut(id.parts())?;
        Some(fiber.poll(context))
    }

    pub fn remove(&mut self, id: TaskId<Tag>) -> bool {
        remove_catching_drop_panic(|| self.inner.remove_parts(id.parts()))
    }
}

impl<'d, F, Tag> Drop for Slab<'d, F, Tag>
where
    F: Fiber<'d>,
{
    fn drop(&mut self) {
        for index in 0..self.inner.capacity() as u32 {
            if let Some(task) = self.inner.key(index).map(TaskId::from_key) {
                self.remove(task);
            }
        }
    }
}

#[pin_project(PinnedDrop, !Unpin)]
pub struct FixedSlab<'d, F, const N: usize, Tag = ()>
where
    F: Fiber<'d>,
{
    #[pin]
    inner: FixedPinSlab<F, N, Tag>,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

#[must_use]
pub struct FixedSlabVacantEntry<'a, F, const N: usize, Tag = ()> {
    inner: FixedPinSlabVacantEntry<'a, F, N, Tag>,
}

impl<F, const N: usize, Tag> FixedSlabVacantEntry<'_, F, N, Tag> {
    pub fn insert(self, fiber: F) -> TaskId<Tag> {
        TaskId::from_key(self.inner.insert(fiber))
    }
}

impl<'d, F, const N: usize, Tag> FixedSlab<'d, F, N, Tag>
where
    F: Fiber<'d>,
{
    pub fn new() -> Self {
        const {
            assert!(N > 0, "fiber slab capacity must be > 0");
        }
        Self {
            inner: FixedPinSlab::new(),
            driver: PhantomData,
        }
    }

    pub fn insert(self: Pin<&mut Self>, fiber: F) -> Option<TaskId<Tag>> {
        self.vacant_entry().map(|entry| entry.insert(fiber))
    }

    pub fn vacant_entry(self: Pin<&mut Self>) -> Option<FixedSlabVacantEntry<'_, F, N, Tag>> {
        Some(FixedSlabVacantEntry {
            inner: self.project().inner.vacant_entry()?,
        })
    }

    pub fn poll(
        self: Pin<&mut Self>,
        id: &TaskId<Tag>,
        context: Pin<&mut Context<'_, 'd>>,
    ) -> Option<Poll<F::Output>> {
        let fiber = self.project().inner.get_parts_mut(id.parts())?;
        Some(fiber.poll(context))
    }

    pub fn remove(mut self: Pin<&mut Self>, id: TaskId<Tag>) -> bool {
        remove_catching_drop_panic(|| self.as_mut().project().inner.remove_parts(id.parts()))
    }
}

impl<'d, F, const N: usize, Tag> Default for FixedSlab<'d, F, N, Tag>
where
    F: Fiber<'d>,
{
    fn default() -> Self {
        Self::new()
    }
}

#[pinned_drop]
impl<'d, F, const N: usize, Tag> PinnedDrop for FixedSlab<'d, F, N, Tag>
where
    F: Fiber<'d>,
{
    fn drop(mut self: Pin<&mut Self>) {
        for index in 0..self.as_ref().project_ref().inner.capacity() as u32 {
            if let Some(task) = self
                .as_ref()
                .project_ref()
                .inner
                .key(index)
                .map(TaskId::from_key)
            {
                self.as_mut().remove(task);
            }
        }
    }
}
