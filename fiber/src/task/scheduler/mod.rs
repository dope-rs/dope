use std::{pin, task};

use dope::core::driver::{retained, schedule};
use o3::collections::{self, slab, slab::pinned};

use crate::{abi, context};

mod sealed;

#[pin_project::pin_project]
struct Entry<'d, F, T: Copy> {
    #[pin]
    fiber: F,
    #[pin]
    binding: crate::raw::RootBinding<'d>,
    target: T,
}

impl<'d, F, T: Copy> Entry<'d, F, T> {
    const fn new(fiber: F, target: T) -> Self {
        Self {
            fiber,
            binding: crate::raw::RootBinding::new(),
            target,
        }
    }
}

struct Storage<'d, F, T: Copy, Tag> {
    entries: pinned::Pool<Entry<'d, F, T>, Tag>,
    queue: pin::Pin<Box<crate::raw::ReadyQueue>>,
}

struct Source<'a, 'd> {
    context: pin::Pin<&'a crate::raw::RootBinding<'d>>,
}

impl<'d, F, T: Copy, Tag> Storage<'d, F, T, Tag> {
    fn try_with_capacity(capacity: slab::Capacity) -> Result<Self, collections::AllocationError> {
        use o3::collections::BoxExt;

        let entries = pinned::Pool::try_with_capacity(capacity)?;
        let queue = Box::into_pin(BoxExt::try_box(crate::raw::ReadyQueue::try_with_capacity(
            capacity,
        )?)?);
        Ok(Self { entries, queue })
    }

    fn insert(
        &mut self,
        fiber: F,
        target: T,
        parent: context::RootWaker<'d>,
    ) -> Option<crate::TaskKey<'d, Tag>> {
        let key = self.entries.insert(Entry::new(fiber, target)).ok()?;
        let parts = key.parts();
        let index = parts.index();
        let bound = {
            let Some(entry) = self.entries.parts(parts) else {
                use std::process::abort;
                abort();
            };
            crate::raw::RootBinding::bind_root(
                Source {
                    context: entry.project_ref().binding,
                },
                self.queue.as_ref(),
                index as usize,
                (),
                parent,
            )
            .is_some()
        };
        if !bound {
            if !self.entries.remove_parts(parts) {
                use std::process::abort;
                abort();
            }
            return None;
        }
        if !self.queue.as_ref().return_ready(index) {
            use std::process::abort;
            abort();
        }
        parent.wake();
        Some(crate::TaskKey::from_key(key))
    }

    fn wake(&self, id: &crate::TaskKey<'d, Tag>) -> bool {
        let Some(entry) = self.entries.parts(id.parts()) else {
            return false;
        };
        let Some(parent) = crate::raw::RootBinding::root_parent(Source {
            context: entry.project_ref().binding,
        }) else {
            return false;
        };
        if self.queue.as_ref().return_ready(id.raw_index()) {
            parent.wake();
        }
        true
    }

    fn is_empty(&self) -> bool {
        self.queue.as_ref().is_empty()
    }

    fn arm_one(&mut self) {
        if self.queue.as_ref().is_empty() {
            return;
        }
        let Some(ready) = self.queue.as_ref().snapshot() else {
            use std::process::abort;
            abort();
        };
        let Some(index) = ready.peek() else {
            use std::process::abort;
            abort();
        };
        let Some((_, entry)) = self.entries.index_mut(index) else {
            use std::process::abort;
            abort();
        };
        let Some(parent) = crate::raw::RootBinding::root_parent(Source {
            context: entry.as_ref().project_ref().binding,
        }) else {
            use std::process::abort;
            abort();
        };
        ready.pause();
        parent.wake();
    }

    fn remove(&mut self, id: crate::TaskKey<'d, Tag>) -> bool {
        let was_ready = self.queue.as_ref().contains(id.raw_index());
        if !self.entries.remove_parts(id.parts()) {
            return false;
        }
        if was_ready {
            self.arm_one();
        }
        true
    }
}

/// A fiber slab whose persistent wake nodes and ready queue share one owner.
pub struct Scheduler<'d, F, T: Copy = usize, Tag = ()>
where
    F: abi::Fiber<'d>,
{
    storage: Storage<'d, F, T, Tag>,
}

impl<'d, F, T, Tag> Scheduler<'d, F, T, Tag>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    pub fn try_with_capacity(
        capacity: slab::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            storage: Storage::try_with_capacity(capacity)?,
        })
    }

    /// Inserts, binds, and schedules a task as one transaction.
    /// Failed child admission removes the task before exposing it.
    pub fn insert(
        &mut self,
        fiber: F,
        target: T,
        parent: context::RootWaker<'d>,
    ) -> Option<crate::TaskKey<'d, Tag>> {
        self.storage.insert(fiber, target, parent)
    }

    /// Polls one stable ready batch within the application-work budget.
    /// An incomplete batch reactivates the next member's exact parent.
    pub fn drive_ready(
        &mut self,
        work: schedule::Application<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        mut complete: impl FnMut(T, F::Output),
    ) -> usize {
        if self.storage.is_empty() {
            return 0;
        }

        let storage = &mut self.storage;
        let Some(mut ready) = storage.queue.as_ref().snapshot() else {
            use std::process::abort;
            abort();
        };
        let mut completed = 0;
        loop {
            let (index, permit) = match work.admit_next(&mut ready) {
                schedule::ApplicationAdmission::Item(index, permit) => (index, permit),
                schedule::ApplicationAdmission::Empty => {
                    drop(ready);
                    storage.arm_one();
                    return completed;
                }
                schedule::ApplicationAdmission::Exhausted(index) => {
                    let Some((_, entry)) = storage.entries.index_mut(index) else {
                        use std::process::abort;
                        abort();
                    };
                    let Some(parent) = crate::raw::RootBinding::root_parent(Source {
                        context: entry.as_ref().project_ref().binding,
                    }) else {
                        use std::process::abort;
                        abort();
                    };
                    ready.pause();
                    parent.wake();
                    return completed;
                }
            };
            let (key, target, output) = {
                let Some((key, mut entry)) = storage.entries.index_mut(index) else {
                    use std::process::abort;
                    abort();
                };
                let entry_ref = entry.as_ref().project_ref();
                let target = *entry_ref.target;
                let Some((parent, wake)) = crate::raw::RootBinding::root_poll(Source {
                    context: entry_ref.binding,
                }) else {
                    use std::process::abort;
                    abort();
                };
                let mut context = pin::pin!(context::Context::from_waker(
                    wake,
                    parent,
                    work,
                    driver.reborrow(),
                ));
                let poll = context
                    .as_mut()
                    .poll_admitted(entry.as_mut().project().fiber, permit);
                let task::Poll::Ready(output) = poll else {
                    continue;
                };
                (key, target, output)
            };
            if !storage.entries.remove_parts(key.parts()) {
                use std::process::abort;
                abort();
            }
            completed += 1;
            complete(target, output);
        }
    }

    pub fn wake(&self, id: &crate::TaskKey<'d, Tag>) -> bool {
        self.storage.wake(id)
    }

    pub fn is_idle(&self) -> bool {
        self.storage.is_empty()
    }

    pub const fn len(&self) -> usize {
        self.storage.entries.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.storage.entries.is_empty()
    }

    pub fn remove(&mut self, id: crate::TaskKey<'d, Tag>) -> bool {
        self.storage.remove(id)
    }
}
