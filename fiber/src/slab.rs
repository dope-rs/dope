use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::Poll;

use o3::collections::{FixedPinSlab, PinSlab, SlabKey, SlabKeyParts};

use crate::{Context, Fiber};
use dope::panic::abort_on_drop_panic;

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
        ErasedTaskId { parts: self.parts }
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
}

impl ErasedTaskId {
    pub fn index(self) -> usize {
        self.parts.index() as usize
    }
}

fn poll_fiber<'d, F>(fiber: Pin<&mut F>, context: Pin<&mut Context<'_, 'd>>) -> Poll<F::Output>
where
    F: Fiber<'d> + 'd,
{
    Fiber::poll(fiber, context)
}

fn catch_drop_panic<R>(operation: impl FnOnce() -> R) -> Result<R, ()> {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(value) => Ok(value),
        Err(payload) => {
            abort_on_drop_panic(payload);
            Err(())
        }
    }
}

pub struct Slab<'d, F, Tag = ()>
where
    F: Fiber<'d> + 'd,
{
    inner: ManuallyDrop<PinSlab<F, Tag>>,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

#[must_use]
pub struct SlabVacantEntry<'a, F, Tag = ()> {
    inner: o3::collections::PinSlabVacantEntry<'a, F, Tag>,
}

impl<F, Tag> SlabVacantEntry<'_, F, Tag> {
    pub fn insert(self, fiber: F) -> TaskId<Tag> {
        TaskId::from_key(self.inner.insert(fiber))
    }
}

impl<'d, F, Tag> Slab<'d, F, Tag>
where
    F: Fiber<'d> + 'd,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: ManuallyDrop::new(PinSlab::with_capacity(capacity)),
            driver: PhantomData,
        }
    }

    pub fn insert(&mut self, fiber: F) -> Option<TaskId<Tag>> {
        self.vacant_entry().map(|entry| entry.insert(fiber))
    }

    pub fn vacant_entry(&mut self) -> Option<SlabVacantEntry<'_, F, Tag>> {
        Some(SlabVacantEntry {
            inner: self.inner.vacant_entry()?,
        })
    }

    pub fn poll(
        &mut self,
        id: &TaskId<Tag>,
        context: Pin<&mut Context<'_, 'd>>,
    ) -> Option<Poll<F::Output>> {
        let fiber = self.inner.get_parts_mut(id.parts())?;
        Some(poll_fiber(fiber, context))
    }

    pub fn remove(&mut self, id: TaskId<Tag>) -> bool {
        catch_drop_panic(|| self.inner.remove_parts(id.parts())).unwrap_or(true)
    }
}

impl<'d, F, Tag> Drop for Slab<'d, F, Tag>
where
    F: Fiber<'d> + 'd,
{
    fn drop(&mut self) {
        for index in 0..self.inner.capacity() as u32 {
            if let Some(task) = self.inner.key(index).map(TaskId::from_key) {
                self.remove(task);
            }
        }
        let _ = catch_drop_panic(|| unsafe { ManuallyDrop::drop(&mut self.inner) });
    }
}

pub struct FixedSlab<'d, F, const N: usize, Tag = ()>
where
    F: Fiber<'d> + 'd,
{
    inner: ManuallyDrop<FixedPinSlab<F, N, Tag>>,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

#[must_use]
pub struct FixedSlabVacantEntry<'a, F, const N: usize, Tag = ()> {
    inner: o3::collections::FixedPinSlabVacantEntry<'a, F, N, Tag>,
}

impl<F, const N: usize, Tag> FixedSlabVacantEntry<'_, F, N, Tag> {
    pub fn insert(self, fiber: F) -> TaskId<Tag> {
        TaskId::from_key(self.inner.insert(fiber))
    }
}

impl<'d, F, const N: usize, Tag> FixedSlab<'d, F, N, Tag>
where
    F: Fiber<'d> + 'd,
{
    pub fn new() -> Self {
        const {
            assert!(N > 0, "fiber slab capacity must be > 0");
        }
        Self {
            inner: ManuallyDrop::new(FixedPinSlab::new()),
            driver: PhantomData,
        }
    }

    pub fn insert(self: Pin<&mut Self>, fiber: F) -> Option<TaskId<Tag>> {
        self.vacant_entry().map(|entry| entry.insert(fiber))
    }

    pub fn vacant_entry(self: Pin<&mut Self>) -> Option<FixedSlabVacantEntry<'_, F, N, Tag>> {
        Some(FixedSlabVacantEntry {
            inner: self.inner().vacant_entry()?,
        })
    }

    pub fn poll(
        self: Pin<&mut Self>,
        id: &TaskId<Tag>,
        context: Pin<&mut Context<'_, 'd>>,
    ) -> Option<Poll<F::Output>> {
        let fiber = self.inner().get_parts_mut(id.parts())?;
        Some(poll_fiber(fiber, context))
    }

    pub fn remove(mut self: Pin<&mut Self>, id: TaskId<Tag>) -> bool {
        catch_drop_panic(|| self.as_mut().inner().remove_parts(id.parts())).unwrap_or(true)
    }

    fn inner(self: Pin<&mut Self>) -> Pin<&mut FixedPinSlab<F, N, Tag>> {
        unsafe { self.map_unchecked_mut(|this| &mut *this.inner) }
    }
}

impl<'d, F, const N: usize, Tag> Default for FixedSlab<'d, F, N, Tag>
where
    F: Fiber<'d> + 'd,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, F, const N: usize, Tag> Drop for FixedSlab<'d, F, N, Tag>
where
    F: Fiber<'d> + 'd,
{
    fn drop(&mut self) {
        for index in 0..self.inner.capacity() as u32 {
            if let Some(task) = self.inner.key(index).map(TaskId::from_key) {
                unsafe { Pin::new_unchecked(&mut *self) }.remove(task);
            }
        }
        let _ = catch_drop_panic(|| unsafe { ManuallyDrop::drop(&mut self.inner) });
    }
}
