use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::{Pin, pin};
use std::task::Poll;

use o3::collections::{FixedPinSlab, PinSlab, SlabKey, SlabKeyParts};

use crate::task::{RootWaker, TaskContext, TaskQueue};
use crate::{Context, Fiber};
use dope::DriverContext;
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

/// A fiber slab whose wake nodes live and die with their fibers.
///
/// Unlike a scoped [`crate::TaskBinding`], bindings in this slab may remain
/// active across polls. Removing an entry drops the fiber first, then unbinds
/// its wake node. Dropping the target queue first safely detaches every node.
pub struct TaskSlab<'d, F, T: Copy = usize, Tag = ()>
where
    F: Fiber<'d> + 'd,
{
    // Field order is part of the safety invariant: fibers (which may retain
    // their waker in an async primitive) are dropped before wake contexts.
    fibers: Slab<'d, F, Tag>,
    contexts: Pin<Box<[TaskContext<T>]>>,
}

impl<'d, F, T, Tag> TaskSlab<'d, F, T, Tag>
where
    F: Fiber<'d> + 'd,
    T: Copy,
{
    pub fn with_capacity(capacity: usize, idle: T) -> Self {
        Self {
            fibers: Slab::with_capacity(capacity),
            contexts: Box::into_pin(
                (0..capacity)
                    .map(|_| TaskContext::with_target(idle))
                    .collect::<Box<[_]>>(),
            ),
        }
    }

    fn context(&self, index: usize) -> Option<Pin<&TaskContext<T>>> {
        let context = self.contexts.as_ref().get_ref().get(index)?;
        // SAFETY: pinning the boxed slice pins every context for the lifetime
        // of this slab.
        Some(unsafe { Pin::new_unchecked(context) })
    }

    pub fn insert(&mut self, fiber: F) -> Option<TaskId<Tag>> {
        self.fibers.insert(fiber)
    }

    pub fn bind(
        &self,
        id: &TaskId<Tag>,
        queue: Pin<&TaskQueue<T>>,
        target: T,
        parent: RootWaker<'d>,
    ) -> bool {
        if !self.fibers.inner.contains_parts(id.parts()) {
            return false;
        }
        let Some(context) = self.context(id.index()) else {
            return false;
        };
        if context.is_bound() {
            return false;
        }
        // SAFETY: the slab owns the pinned context and its fiber as one entry.
        // Queue Drop detaches the context, while entry removal drops the fiber
        // before unbinding the context.
        let _ = unsafe { context.bind_inner(queue, target, Some(parent.into_waker())) };
        true
    }

    pub fn poll(
        &mut self,
        id: &TaskId<Tag>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Poll<F::Output>> {
        let index = id.index();
        if !self.fibers.inner.contains_parts(id.parts()) {
            return None;
        }
        let context = self.context(index)?;
        if !context.is_bound() {
            return None;
        }
        // SAFETY: the corresponding fiber is owned by this slab and is
        // dropped before `context` can be unbound or freed.
        let wake = unsafe { context.context_unchecked() };
        let mut context = pin!(Context::from_waker(wake, driver.reborrow()));
        self.fibers.poll(id, context.as_mut())
    }

    pub fn wake(&self, id: &TaskId<Tag>) -> bool {
        if !self.fibers.inner.contains_parts(id.parts()) {
            return false;
        }
        let Some(context) = self.context(id.index()) else {
            return false;
        };
        if !context.is_bound() {
            return false;
        }
        context.wake();
        true
    }

    pub fn remove(&mut self, id: TaskId<Tag>) -> bool {
        let index = id.index();
        if !self.fibers.remove(id) {
            return false;
        }
        let context = self
            .context(index)
            .expect("task slab context must cover every fiber slot");
        // SAFETY: the fiber has just been dropped, so none of its async
        // registrations can retain this wake node.
        unsafe { context.unbind() };
        true
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
