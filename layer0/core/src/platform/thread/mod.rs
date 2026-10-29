use std::{cell, marker, mem, num::NonZero, ptr, vec};

use dope_sys::thread;
use o3::alloc::{self, VecExt};

use crate::Errno;

mod raw;

union Value<T, O> {
    task: mem::ManuallyDrop<T>,
    output: mem::ManuallyDrop<O>,
}

/// A single owned input transferred into a worker and returned as an output.
pub trait Task: Send {
    type Output: Send;
    fn run(self) -> Self::Output;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Attributes(Errno),
    Create(Errno),
}

/// Builds the one allocation retained for the complete worker scope.
pub struct Builder<T, O, P> {
    group: raw::Group<T, O, P>,
}

impl<T, O, P> Builder<T, O, P> {
    pub fn try_new(capacity: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            group: raw::Group {
                slots: Vec::try_new(capacity)?,
                started: 0,
            },
        })
    }
    pub fn try_push(&mut self, task: T, parent: P) -> Result<(), alloc::Error> {
        alloc::Vacant::try_new(&mut self.group.slots)?.insert(Slot {
            value: cell::UnsafeCell::new(Value {
                task: mem::ManuallyDrop::new(task),
            }),
            thread: cell::Cell::new(mem::MaybeUninit::uninit()),
            parent,
        });
        Ok(())
    }
    /// Retains every started worker even if the borrowing view is forgotten.
    ///
    pub fn scope<R>(self, run: impl for<'scope> FnOnce(Workers<'scope, T, O, P>) -> R) -> R {
        let mut group = self.group;
        run(Workers {
            group: &mut group,
            owner: marker::PhantomData,
        })
    }
}

/// A worker's parent-only metadata and private transfer storage.
pub struct Slot<T, O, P> {
    value: cell::UnsafeCell<Value<T, O>>,
    thread: cell::Cell<mem::MaybeUninit<thread::Handle>>,
    parent: P,
}

impl<T, O, P> Slot<T, O, P> {
    pub fn parent(&self) -> &P {
        &self.parent
    }
}

/// Borrowing authority to start and observe the scope-owned workers.
pub struct Workers<'scope, T, O, P> {
    group: &'scope mut raw::Group<T, O, P>,
    owner: marker::PhantomData<fn(&'scope ()) -> &'scope ()>,
}

impl<T, O, P> Workers<'_, T, O, P> {
    pub fn entries(&self) -> &[Slot<T, O, P>] {
        &self.group.slots
    }
    /// Mutably borrows only coordinator metadata for the started worker prefix.
    /// Native entries retain exclusive access to their task and output cells.
    pub fn parents_mut(&mut self) -> impl ExactSizeIterator<Item = &mut P> {
        self.group.parents_mut()
    }
    /// Joins every started worker and hands back their outputs.
    ///
    /// The group retains no started slot afterwards, so its own join pass is empty.
    pub fn finish(self) -> Finished<T, O, P> {
        self.group.take()
    }
}

/// Joined outputs, in the original registration order.
pub struct Finished<T, O, P> {
    slots: vec::IntoIter<Slot<T, O, P>>,
}

impl<T, O, P> Workers<'_, T, O, P> {
    /// Spawns the remaining workers. The outer group owns every slot through
    /// each join, so the context pointer stays valid until its handle joins.
    /// A failure borrows the exact unstarted worker's parent metadata.
    pub fn start(&mut self, stack: NonZero<usize>) -> Result<(), (&P, Error)>
    where
        T: Task<Output = O>,
    {
        // SAFETY: only successful spawns extend started within this fixed
        // allocation; Workers cannot grow or remove slots before finish.
        let pending = unsafe { self.group.slots.get_unchecked(self.group.started..) };
        let Some(first) = pending.first() else {
            return Ok(());
        };
        let attr = thread::Attrs::with_stack(stack)
            .map_err(|error| (&first.parent, Error::Attributes(error)))?;
        for slot in pending {
            // SAFETY: Group owns this fixed slot allocation for the entire
            // scope. The callback alone accesses task/output until join; the
            // parent only accesses the disjoint handle and metadata. Store the
            // handle before any fallible step. Group joins every stored handle
            // before freeing slots, including when Workers is forgotten.
            let entry = unsafe { thread::raw::Entry::new(ptr::NonNull::from(slot)) };
            let handle = entry
                .spawn(&attr)
                .map_err(|error| (&slot.parent, Error::Create(error)))?;
            slot.thread.set(mem::MaybeUninit::new(handle));
            self.group.started += 1;
        }
        Ok(())
    }
}

impl<T, O, P> Iterator for Finished<T, O, P> {
    type Item = (P, O);

    /// Yields each started worker's output; a started worker stores exactly one
    /// output before its handle joins, so every retained slot below has one.
    fn next(&mut self) -> Option<Self::Item> {
        let slot = self.slots.next()?;
        let Slot { value, parent, .. } = slot;
        // SAFETY: the group joined this worker, and the worker stores exactly one
        // output before returning, so the cell holds an initialized value.
        Some((
            parent,
            mem::ManuallyDrop::into_inner(unsafe { value.into_inner().output }),
        ))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slots.size_hint()
    }
}
impl<T, O, P> ExactSizeIterator for Finished<T, O, P> {}

impl<T, O, P> Drop for Finished<T, O, P> {
    fn drop(&mut self) {
        for slot in self.slots.as_mut_slice() {
            // SAFETY: the group joins every retained slot, and next removes
            // each yielded slot. Every remaining output is initialized and owned.
            unsafe { mem::ManuallyDrop::drop(&mut slot.value.get_mut().output) };
        }
    }
}
