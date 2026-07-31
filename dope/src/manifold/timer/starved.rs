use std::cell::Cell;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::NonNull;
use std::time::Instant;

use dope_core::driver::ready::{CompletionSlot, CompletionWaker};
use o3::collections::intrusive::{AvlAdapter, AvlNode, AvlTree};
use pin_project::pin_project;

#[pin_project]
#[repr(C)]
pub struct StarvedWaiter<'d> {
    #[pin]
    node: AvlNode,
    wake: CompletionSlot<'d>,
    deadline: Cell<Instant>,
    queued: Cell<bool>,
}

impl StarvedWaiter<'_> {
    pub fn new() -> Self {
        Self {
            node: AvlNode::new(),
            wake: CompletionSlot::empty(),
            deadline: Cell::new(Instant::now()),
            queued: Cell::new(false),
        }
    }
}

impl Default for StarvedWaiter<'_> {
    fn default() -> Self {
        Self::new()
    }
}

struct WaiterAdapter<'d>(PhantomData<fn(&'d ()) -> &'d ()>);

// SAFETY: StarvedWaiter is repr(C), `node` is its leading structurally pinned
// field, and casting that node pointer back recovers the exact pinned owner.
unsafe impl<'d> AvlAdapter for WaiterAdapter<'d> {
    type Value = StarvedWaiter<'d>;

    fn node(waiter: Pin<&Self::Value>) -> Pin<&AvlNode> {
        waiter.project_ref().node
    }

    unsafe fn from_node(node: NonNull<AvlNode>) -> NonNull<StarvedWaiter<'d>> {
        node.cast()
    }
}

pub(crate) struct StarvedQueue<'d> {
    tree: AvlTree<WaiterAdapter<'d>>,
}

impl<'d> StarvedQueue<'d> {
    pub(crate) fn new() -> Self {
        Self {
            tree: AvlTree::new(),
        }
    }

    pub(crate) fn register(
        &self,
        waiter: Pin<&StarvedWaiter<'d>>,
        deadline: Instant,
        wake: CompletionWaker<'d>,
    ) {
        let waiter_ref = waiter.get_ref();
        waiter_ref.wake.set(wake);
        if waiter_ref.queued.get() {
            return;
        }
        waiter_ref.deadline.set(deadline);
        waiter_ref.queued.set(true);
        unsafe { self.tree.insert(waiter, Self::before) };
    }

    pub(crate) fn unregister(&self, waiter: Pin<&StarvedWaiter<'d>>) {
        let waiter_ref = waiter.get_ref();
        if waiter_ref.queued.get() {
            unsafe { self.tree.remove(waiter) };
            waiter_ref.queued.set(false);
        }
        waiter_ref.wake.clear();
    }

    pub(crate) fn wake_min(&self) {
        let Some(waiter) = self.tree.first() else {
            return;
        };
        unsafe { self.tree.remove(waiter) };
        let waiter = waiter.get_ref();
        waiter.queued.set(false);
        waiter.wake.wake();
    }

    pub(crate) fn expire(&self, now: Instant) {
        while let Some(waiter) = self.tree.first() {
            if waiter.deadline.get() > now {
                break;
            }
            unsafe { self.tree.remove(waiter) };
            let waiter = waiter.get_ref();
            waiter.queued.set(false);
            waiter.wake.wake();
        }
    }

    pub(crate) fn min_deadline(&self) -> Option<Instant> {
        self.tree.first().map(|waiter| waiter.deadline.get())
    }

    fn before(first: &StarvedWaiter<'d>, second: &StarvedWaiter<'d>) -> bool {
        let first_key = (first.deadline.get(), std::ptr::from_ref(first) as usize);
        let second_key = (second.deadline.get(), std::ptr::from_ref(second) as usize);
        first_key < second_key
    }
}
