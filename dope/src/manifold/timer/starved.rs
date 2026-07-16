use std::cell::Cell;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::NonNull;
use std::time::Instant;

use dope_core::driver::ready::CompletionWaker;
use o3::collections::intrusive::{AvlNode, AvlTree};

#[repr(C)]
pub struct Waiter<'d> {
    node: AvlNode,
    wake: Cell<Option<CompletionWaker<'d>>>,
    deadline: Cell<Instant>,
    queued: Cell<bool>,
    _driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl Waiter<'_> {
    pub fn new() -> Self {
        Self {
            node: AvlNode::new(),
            wake: Cell::new(None),
            deadline: Cell::new(Instant::now()),
            queued: Cell::new(false),
            _driver: PhantomData,
        }
    }

    fn node(self: Pin<&Self>) -> Pin<&AvlNode> {
        unsafe { self.map_unchecked(|waiter| &waiter.node) }
    }
}

impl Default for Waiter<'_> {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct StarvedTree<'d> {
    tree: AvlTree,
    _marker: PhantomData<&'d ()>,
}

impl<'d> StarvedTree<'d> {
    pub(crate) fn new() -> Self {
        Self {
            tree: AvlTree::new(),
            _marker: PhantomData,
        }
    }

    pub(crate) fn register(
        &self,
        waiter: Pin<&Waiter<'d>>,
        deadline: Instant,
        wake: CompletionWaker<'d>,
    ) {
        let node = waiter.node();
        let waiter = waiter.get_ref();
        waiter.wake.set(Some(wake));
        if waiter.queued.get() {
            return;
        }
        waiter.deadline.set(deadline);
        waiter.queued.set(true);
        unsafe { self.tree.insert(node, Self::before) };
    }

    pub(crate) fn unregister(&self, waiter: Pin<&Waiter<'d>>) {
        let waiter = waiter.get_ref();
        if waiter.queued.get() {
            unsafe { self.tree.remove(NonNull::from(&waiter.node)) };
            waiter.queued.set(false);
        }
        waiter.wake.set(None);
    }

    pub(crate) fn wake_min(&self) {
        let Some(min) = self.tree.first() else {
            return;
        };
        let waiter = unsafe { Self::waiter(min) };
        unsafe { self.tree.remove(min) };
        waiter.queued.set(false);
        if let Some(wake) = waiter.wake.get() {
            wake.wake();
        }
    }

    pub(crate) fn expire(&self, now: Instant) {
        while let Some(min) = self.tree.first() {
            let waiter = unsafe { Self::waiter(min) };
            if waiter.deadline.get() > now {
                break;
            }
            unsafe { self.tree.remove(min) };
            waiter.queued.set(false);
            if let Some(wake) = waiter.wake.get() {
                wake.wake();
            }
        }
    }

    pub(crate) fn min_deadline(&self) -> Option<Instant> {
        self.tree
            .first()
            .map(|node| unsafe { Self::waiter(node) }.deadline.get())
    }

    fn before(a: NonNull<AvlNode>, b: NonNull<AvlNode>) -> bool {
        let (first, second) = unsafe { (Self::waiter(a), Self::waiter(b)) };
        (first.deadline.get(), a.as_ptr() as usize) < (second.deadline.get(), b.as_ptr() as usize)
    }

    /// # Safety
    /// `node` is the leading intrusive AVL `AvlNode` field of a live `Waiter`.
    unsafe fn waiter<'a>(node: NonNull<AvlNode>) -> &'a Waiter<'d> {
        unsafe { node.cast().as_ref() }
    }
}
