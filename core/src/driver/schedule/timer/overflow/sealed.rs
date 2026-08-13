use std::{pin, time};

use o3::collections::intrusive::avl;

use crate::driver::schedule::{
    credits,
    ready::completion,
    timer::{self, overflow},
};

pub(in crate::driver::schedule::timer) struct Queue<'d> {
    tree: avl::raw::Tree<overflow::State<'d>>,
}

impl<'d> Queue<'d> {
    pub(in crate::driver::schedule::timer::overflow) fn new() -> Self {
        use o3::collections::intrusive::avl::raw::Tree;
        Self { tree: Tree::new() }
    }

    pub(in crate::driver::schedule::timer::overflow) fn register(
        &self,
        waiter: pin::Pin<&avl::raw::Entry<overflow::State<'d>>>,
        deadline: time::Instant,
        wake: completion::Waker<'d>,
    ) {
        let waiter_ref = waiter.value();
        waiter_ref.wake.set(wake);
        if waiter.is_linked() {
            return;
        }
        waiter_ref.deadline.set(deadline);
        self.link(waiter);
    }

    pub(in crate::driver::schedule::timer::overflow) fn unregister(
        &self,
        waiter: pin::Pin<&avl::raw::Entry<overflow::State<'d>>>,
    ) -> bool {
        let linked = waiter.is_linked();
        let notified = !waiter.value().wake.is_empty();
        if linked {
            self.unlink(waiter);
        }
        waiter.value().wake.clear();
        linked || notified
    }

    pub(in crate::driver::schedule::timer::overflow) fn wake_min(&self) {
        let Some(waiter) = self.pop_first() else {
            return;
        };
        waiter.value().wake.wake();
    }

    pub(in crate::driver::schedule::timer::overflow) fn expire(
        &self,
        now: time::Instant,
        budget: &mut credits::Budget<'_, 'd, timer::Lane>,
    ) {
        while let Some(waiter) = self.tree.first_entry() {
            if waiter.value().deadline.get() > now {
                break;
            }
            if !budget.take() {
                break;
            }
            self.unlink(waiter);
            waiter.value().wake.wake();
        }
    }

    pub(in crate::driver::schedule::timer::overflow) fn min_deadline(
        &self,
    ) -> Option<time::Instant> {
        self.tree
            .first_entry()
            .map(|waiter| waiter.value().deadline.get())
    }

    fn link(&self, waiter: pin::Pin<&avl::raw::Entry<overflow::State<'d>>>) {
        // SAFETY: this driver's unique queue links each pinned entry once.
        unsafe { self.tree.insert_entry(waiter, Self::before) };
    }

    fn unlink(&self, waiter: pin::Pin<&avl::raw::Entry<overflow::State<'d>>>) {
        // SAFETY: linked entries belong to this driver's unique queue.
        unsafe { self.tree.remove_entry(waiter) };
    }

    fn pop_first(&self) -> Option<pin::Pin<&avl::raw::Entry<overflow::State<'d>>>> {
        let waiter = self.tree.first_entry()?;
        self.unlink(waiter);
        Some(waiter)
    }

    fn before(first: &overflow::State<'d>, second: &overflow::State<'d>) -> bool {
        use std::ptr::from_ref;
        let first_key = (first.deadline.get(), from_ref(first) as usize);
        let second_key = (second.deadline.get(), from_ref(second) as usize);
        first_key < second_key
    }
}
