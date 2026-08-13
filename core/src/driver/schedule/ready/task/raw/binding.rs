use std::{mem, pin, process, ptr};

use o3::{cell, collections::batch::set};

use crate::driver::schedule::ready::{self, completion, task};

#[derive(Clone, Copy)]
pub struct Binding<'d> {
    ready: cell::StableLink<set::Set<usize>>,
    index: usize,
    parent: completion::Wake<'d>,
    child: ready::DynamicKey<'d>,
}

impl<'d> Binding<'d> {
    fn child_wake(self) -> completion::Wake<'d> {
        completion::Wake(self.parent.0.retarget(self.child.key()))
    }
}

impl<'d> Binding<'d> {
    /// Reserves the exact child node when its wake chain fits the ceiling.
    #[must_use]
    pub fn admit<'a>(
        parent: completion::Wake<'d>,
        node: pin::Pin<&'a task::Node<'d>>,
    ) -> Option<task::Admission<'a, 'a, 'd>>
    where
        'd: 'a,
    {
        if !parent.0.task_hops_at_most(task::MAX_WAKE_HOPS - 1) {
            return None;
        }
        let child = parent.0.reserve_task()?;
        Some(task::Admission::global(node, parent, child))
    }

    /// # Safety
    /// Keep the node and `ready` pinned until `unbind`; `index` must satisfy
    /// the ready set's typed owner and capacity.
    pub unsafe fn bind<'lease, 'node>(
        admission: task::Admission<'lease, 'node, 'd>,
        ready: pin::Pin<&set::Set<usize>>,
        index: usize,
    ) -> completion::Wake<'d> {
        if index >= ready.capacity() || admission.node.binding.get().is_some() {
            process::abort();
        }
        let admission = mem::ManuallyDrop::new(admission);
        unsafe { Self::bind_parts(&admission, ready, index) }
    }

    /// # Safety
    /// Keep the node and `ready` pinned until `unbind`; `index` must satisfy
    /// the ready set's typed owner and capacity.
    pub unsafe fn bind_leased<'lease, 'node>(
        admission: task::Lease<'lease, 'node, 'd>,
        ready: pin::Pin<&set::Set<usize>>,
        index: usize,
    ) -> completion::Wake<'d> {
        if index >= ready.capacity() || admission.admission.node.binding.get().is_some() {
            process::abort();
        }
        let admission = mem::ManuallyDrop::new(admission);
        unsafe { Self::bind_parts(&admission.admission, ready, index) }
    }

    unsafe fn bind_parts<'lease, 'node>(
        admission: &task::Admission<'lease, 'node, 'd>,
        ready: pin::Pin<&set::Set<usize>>,
        index: usize,
    ) -> completion::Wake<'d> {
        let node = admission.node;
        let parent = admission.parent;
        let reservation = unsafe { ptr::read(&admission.child) };
        let child = unsafe { parent.0.install_task(reservation, node) };
        let binding = Binding {
            // SAFETY: forwarded from this function's caller proof.
            ready: unsafe { retain_link(ready) },
            index,
            parent,
            child,
        };
        let wake = binding.child_wake();
        node.binding.set(Some(binding));
        wake
    }

    /// Revokes the node's generational wake slot before unlinking its queue.
    /// A wake already queued for the retiring child is promoted to its live
    /// parent so teardown cannot silently consume the notification.
    pub fn unbind(node: pin::Pin<&task::Node<'d>>) -> Option<usize> {
        let binding = node.binding.take()?;
        let wake_parent = binding.parent.0.release(binding.child);
        binding.ready.get().remove(binding.index);
        if wake_parent {
            binding.parent.wake();
        }
        Some(binding.index)
    }

    fn reclaim(
        node: pin::Pin<&task::Node<'d>>,
        parent: completion::Wake<'d>,
    ) -> Option<(usize, ready::Reservation<'d>)> {
        let binding = node.binding.get()?;
        if binding.parent != parent {
            return None;
        }
        let binding = node.binding.take()?;
        let (reservation, wake_parent) = binding.parent.0.reclaim_task(binding.child);
        binding.ready.get().remove(binding.index);
        if wake_parent {
            binding.parent.wake();
        }
        reservation.map(|reservation| (binding.index, reservation))
    }

    /// # Safety
    /// `node` must be a live binding admitted by this exact `domain`; equal
    /// parents and capacities do not prove owner identity.
    #[must_use]
    pub unsafe fn reclaim_domain<Tag, const N: usize>(
        domain: &mut task::Domain<'d, Tag, N>,
        node: pin::Pin<&task::Node<'d>>,
    ) -> Option<usize> {
        let (bound_index, reservation) = Self::reclaim(node, domain.parent())?;
        domain.reclaim(reservation);
        Some(bound_index)
    }

    pub fn is_bound(node: pin::Pin<&task::Node<'d>>) -> bool {
        node.binding.get().is_some()
    }

    pub(in crate::driver::schedule::ready) fn activate(
        node: pin::Pin<&task::Node<'d>>,
    ) -> Option<completion::Wake<'d>> {
        let binding = node.binding.get()?;
        if binding.ready.get().insert(binding.index) {
            Some(binding.parent)
        } else {
            None
        }
    }

    pub(in crate::driver::schedule::ready) fn is_ready(node: pin::Pin<&task::Node<'d>>) -> bool {
        node.binding
            .get()
            .is_some_and(|binding| binding.ready.get().contains(binding.index))
    }

    pub(in crate::driver::schedule::ready) fn parent(
        node: pin::Pin<&task::Node<'d>>,
    ) -> Option<completion::Wake<'d>> {
        node.binding.get().map(|binding| binding.parent)
    }

    /// # Safety
    /// The binding must have been installed with a wake target that could not
    /// name a task entry.
    #[doc(hidden)]
    pub unsafe fn root_parent_unchecked(
        node: pin::Pin<&task::Node<'d>>,
    ) -> Option<ready::Target<'d>> {
        let parent = node.binding.get()?.parent;
        Some(unsafe { parent.0.into_root_target() })
    }

    /// # Safety
    /// The binding must have been installed with a wake target that could not
    /// name a task entry.
    #[doc(hidden)]
    pub unsafe fn root_poll_unchecked(
        node: pin::Pin<&task::Node<'d>>,
    ) -> Option<(ready::Target<'d>, completion::Wake<'d>)> {
        let binding = node.binding.get()?;
        let parent = unsafe { binding.parent.0.into_root_target() };
        Some((parent, binding.child_wake()))
    }

    pub fn waker<'a>(node: pin::Pin<&'a task::Node<'d>>) -> Option<completion::Wake<'d>>
    where
        'd: 'a,
    {
        node.binding.get().map(Binding::child_wake)
    }

    pub fn wake(node: pin::Pin<&task::Node<'d>>) -> bool {
        let Some(wake) = Self::waker(node) else {
            return false;
        };
        wake.wake();
        true
    }
}

/// # Safety
/// `ready` must remain pinned and live until the returned link and every copy
/// stored in a binding have been revoked.
unsafe fn retain_link(ready: pin::Pin<&set::Set<usize>>) -> cell::StableLink<set::Set<usize>> {
    struct Source(ptr::NonNull<set::Set<usize>>);

    // SAFETY: Source is local to this unsafe function, whose caller supplies
    // the complete pin and liveness contract for every resulting link.
    unsafe impl cell::raw::StableLinkSource<set::Set<usize>> for Source {
        fn pointer(self) -> ptr::NonNull<set::Set<usize>> {
            self.0
        }
    }

    cell::StableLink::from_stable(Source(ptr::NonNull::from(ready.get_ref())))
}
