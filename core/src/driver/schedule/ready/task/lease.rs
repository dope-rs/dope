use std::{mem, pin, process};

use crate::driver::schedule::ready::{self, completion, task};

/// Exact domain admission whose unused credit returns through its lease.
#[doc(hidden)]
#[must_use = "an admitted task must be bound or released"]
pub struct Lease<'lease, 'node, 'd> {
    pub(in crate::driver::schedule::ready::task) admission:
        mem::ManuallyDrop<task::Admission<'lease, 'node, 'd>>,
    remaining: &'lease mut usize,
}

#[must_use = "task credits return when dropped"]
pub(super) struct Credits<'d> {
    parent: completion::Wake<'d>,
    remaining: usize,
}

impl<'lease, 'node, 'd> Lease<'lease, 'node, 'd> {
    fn new(
        node: pin::Pin<&'node task::Node<'d>>,
        parent: completion::Wake<'d>,
        child: ready::Reservation<'d>,
        remaining: &'lease mut usize,
    ) -> Self {
        Self {
            admission: mem::ManuallyDrop::new(task::Admission::leased(node, parent, child)),
            remaining,
        }
    }
}

impl<'d> Credits<'d> {
    pub(super) fn try_new(
        parent: completion::Wake<'d>,
        requested: usize,
    ) -> Result<Self, task::Error> {
        if !parent.0.task_hops_at_most(task::MAX_WAKE_HOPS - 1) {
            return Err(task::Error::WakeHopCeiling);
        }
        parent
            .0
            .lease_tasks(requested)
            .map_err(|available| task::Error::Capacity {
                requested,
                available,
            })?;
        Ok(Self {
            parent,
            remaining: requested,
        })
    }

    #[must_use]
    pub(super) fn admit<'lease, 'node>(
        &'lease mut self,
        node: pin::Pin<&'node task::Node<'d>>,
    ) -> Option<task::Lease<'lease, 'node, 'd>> {
        let remaining = self.remaining.checked_sub(1)?;
        let Some(child) = self.parent.0.claim_leased_task() else {
            process::abort();
        };
        self.remaining = remaining;
        Some(task::Lease::new(
            node,
            self.parent,
            child,
            &mut self.remaining,
        ))
    }

    pub(super) fn reclaim(&mut self, child: ready::Reservation<'d>) {
        self.parent.0.return_leased_task(&child);
        let Some(remaining) = self.remaining.checked_add(1) else {
            process::abort();
        };
        self.remaining = remaining;
    }

    pub(super) fn parent(&self) -> completion::Wake<'d> {
        self.parent
    }

    pub(super) fn wake_parent(&self) {
        self.parent.wake();
    }

    pub(super) fn retarget(&mut self, parent: completion::Wake<'d>, capacity: usize) -> bool {
        if self.remaining != capacity {
            return false;
        }
        if self.parent == parent {
            return true;
        }
        if !self.parent.same_driver(parent) || !parent.0.task_hops_at_most(task::MAX_WAKE_HOPS - 1)
        {
            return false;
        }
        self.parent = parent;
        true
    }
}

impl Drop for Lease<'_, '_, '_> {
    fn drop(&mut self) {
        self.admission
            .parent
            .0
            .return_leased_task(&self.admission.child);
        let Some(next) = self.remaining.checked_add(1) else {
            process::abort();
        };
        *self.remaining = next;
    }
}

impl Drop for Credits<'_> {
    fn drop(&mut self) {
        self.parent.0.release_task_lease(self.remaining);
    }
}

const _: () = assert!(
    core::mem::size_of::<Credits<'static>>()
        == core::mem::size_of::<completion::Wake<'static>>() + core::mem::size_of::<usize>()
);
const _: () =
    assert!(mem::size_of::<Lease<'static, 'static, 'static>>() == 5 * mem::size_of::<usize>());
