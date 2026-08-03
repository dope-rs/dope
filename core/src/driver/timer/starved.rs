use std::cell::Cell;
use std::pin::Pin;
use std::time::Instant;

use o3::collections::intrusive::{AvlEntry, AvlEntryTree};
use pin_project::{pin_project, pinned_drop};

use super::Timer;
use crate::driver::ready::{CompletionSlot, CompletionWaker};

#[pin_project(PinnedDrop)]
pub(super) struct Waiter<'timer, 'd> {
    timer: &'timer Timer<'d>,
    #[pin]
    entry: Entry<'d>,
}

impl<'timer, 'd> Waiter<'timer, 'd> {
    pub(super) fn new(timer: &'timer Timer<'d>) -> Self {
        Self {
            timer,
            entry: AvlEntry::new(WaiterState::new()),
        }
    }

    pub(super) fn register(self: Pin<&Self>, deadline: Instant, wake: CompletionWaker<'d>) {
        let this = self.project_ref();
        this.timer.register_waiter(this.entry, deadline, wake);
    }

    pub(super) fn unregister(self: Pin<&Self>) {
        let this = self.project_ref();
        this.timer.unregister_waiter(this.entry);
    }

    pub(super) fn timer(self: Pin<&Self>) -> &'timer Timer<'d> {
        self.project_ref().timer
    }
}

#[pinned_drop]
impl PinnedDrop for Waiter<'_, '_> {
    fn drop(self: Pin<&mut Self>) {
        self.as_ref().unregister();
    }
}

pub(super) struct WaiterState<'d> {
    wake: CompletionSlot<'d>,
    deadline: Cell<Instant>,
}

impl WaiterState<'_> {
    fn new() -> Self {
        Self {
            wake: CompletionSlot::empty(),
            deadline: Cell::new(Instant::now()),
        }
    }
}

pub(super) type Entry<'d> = AvlEntry<WaiterState<'d>>;

pub(super) struct Queue<'d> {
    tree: AvlEntryTree<WaiterState<'d>>,
}

impl<'d> Queue<'d> {
    pub(super) fn new() -> Self {
        Self {
            tree: AvlEntryTree::new(),
        }
    }

    pub(super) fn register(
        &self,
        waiter: Pin<&Entry<'d>>,
        deadline: Instant,
        wake: CompletionWaker<'d>,
    ) {
        let waiter_ref = waiter.value();
        waiter_ref.wake.set(wake);
        if waiter.is_linked() {
            return;
        }
        waiter_ref.deadline.set(deadline);
        self.link(waiter);
    }

    pub(super) fn unregister(&self, waiter: Pin<&Entry<'d>>) -> bool {
        let linked = waiter.is_linked();
        let notified = !waiter.value().wake.is_empty();
        if linked {
            self.unlink(waiter);
        }
        waiter.value().wake.clear();
        linked || notified
    }

    pub(super) fn wake_min(&self) {
        let Some(waiter) = self.pop_first() else {
            return;
        };
        waiter.value().wake.wake();
    }

    pub(super) fn expire(&self, now: Instant) {
        while let Some(waiter) = self.tree.first_entry() {
            if waiter.value().deadline.get() > now {
                break;
            }
            self.unlink(waiter);
            waiter.value().wake.wake();
        }
    }

    pub(super) fn min_deadline(&self) -> Option<Instant> {
        self.tree
            .first_entry()
            .map(|waiter| waiter.value().deadline.get())
    }

    fn link(&self, waiter: Pin<&Entry<'d>>) {
        // SAFETY: this driver's unique queue links each pinned entry once.
        unsafe { self.tree.insert_entry(waiter, Self::before) };
    }

    fn unlink(&self, waiter: Pin<&Entry<'d>>) {
        // SAFETY: linked entries belong to this driver's unique queue.
        unsafe { self.tree.remove_entry(waiter) };
    }

    fn pop_first(&self) -> Option<Pin<&Entry<'d>>> {
        let waiter = self.tree.first_entry()?;
        self.unlink(waiter);
        Some(waiter)
    }

    fn before(first: &WaiterState<'d>, second: &WaiterState<'d>) -> bool {
        let first_key = (first.deadline.get(), std::ptr::from_ref(first) as usize);
        let second_key = (second.deadline.get(), std::ptr::from_ref(second) as usize);
        first_key < second_key
    }
}
