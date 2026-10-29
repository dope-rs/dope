use std::{cell, marker, mem, num, pin::Pin, ptr};

use o3::{base::marker::ThreadBound, depot, mem::quota};

use crate::{
    context,
    core::{
        driver,
        driver::{
            route::table::conn,
            schedule::{app, ready::wake},
        },
    },
};

mod capacity;
mod index;
mod length;
mod slots;
mod target;

pub use capacity::Capacity;
pub use index::Index;
pub use length::Length;
pub use slots::Slots;
use target::{Active, Target};

/// Exact registration state of a caller-owned waiter.
#[must_use = "registration state must be consumed"]
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Registration {
    Vacant,
    Active,
}

const _: () = assert!(core::mem::size_of::<Registration>() == core::mem::size_of::<u8>());

/// Exact reason a caller-owned waiter could not be registered.
pub enum Error<'d> {
    Foreign,
    QueueFull {
        capacity: Capacity,
        wake: wake::Waker<'d>,
    },
    SlotFull(wake::Waker<'d>),
    Index {
        index: Index,
        slots: Slots,
    },
}

/// Exact registration failure without moving the caller's retained wake capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause {
    Foreign,
    QueueFull { capacity: Capacity },
    SlotFull,
    Index { index: Index, slots: Slots },
}

impl Error<'_> {
    pub const fn cause(&self) -> Cause {
        match self {
            Self::Foreign => Cause::Foreign,
            Self::QueueFull { capacity, .. } => Cause::QueueFull {
                capacity: *capacity,
            },
            Self::SlotFull(_) => Cause::SlotFull,
            Self::Index { index, slots } => Cause::Index {
                index: *index,
                slots: *slots,
            },
        }
    }
}

#[must_use = "Pending means the wake did not fit in this application turn"]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WakeStatus {
    Complete,
    Pending,
}

/// An inline bounded FIFO of caller-owned waiters with O(1) operations.
///
/// ```compile_fail
/// use std::pin::{Pin, pin};
/// use dope::core::driver;
/// use dope_fiber::{context::Context, wait::{Capacity, Queue, Waiter}};
///
/// fn target_cannot_drop_first<'poll, 'd>(
///     driver: driver::Reference<'d>,
///     context: Pin<&Context<'poll, 'd>>,
/// ) {
///     let waiter = pin!(Waiter::new());
///     {
///         let queue = Queue::new(driver, Capacity::new(1));
///         assert!(queue.try_register(waiter.as_ref(), context).is_ok());
///     }
///     waiter.as_ref().unregister();
/// }
/// ```
/// ```compile_fail
/// use std::pin::{Pin, pin};
/// use dope::core::driver;
/// use dope_fiber::{context::Context, wait::{Capacity, Queue, Waiter}};
/// fn target_cannot_move<'poll, 'd>(driver: driver::Reference<'d>, context: Pin<&Context<'poll, 'd>>) {
///     let queue = Queue::new(driver, Capacity::new(1));
///     let waiter = pin!(Waiter::new());
///     assert!(queue.try_register(waiter.as_ref(), context).is_ok());
///     let moved = queue;
///     waiter.as_ref().unregister();
///     assert_eq!(moved.len().get(), 0);
/// }
/// ```
#[repr(align(2))]
pub struct Queue<'d> {
    links: depot::raw::Links<cell::Cell<Option<Active<'d>>>>,
    len: cell::Cell<usize>,
    capacity: Capacity,
    driver: driver::Reference<'d>,
}

impl<'d> Queue<'d> {
    pub const fn new(driver: driver::Reference<'d>, capacity: Capacity) -> Self {
        Self {
            links: depot::raw::Links::new(),
            len: cell::Cell::new(0),
            capacity,
            driver,
        }
    }

    pub const fn for_conns(driver: driver::Reference<'d>, capacity: conn::Capacity) -> Self {
        Self::new(driver, Capacity::new(capacity.count().get() as usize))
    }

    pub fn try_register<'target, 'poll>(
        &'target self,
        waiter: Pin<&Waiter<'target, 'd>>,
        context: Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), Error<'d>> {
        if !context.waker().belongs_to(self.driver) {
            return Err(Error::Foreign);
        }
        let target = Target::queue(self);
        let Some(wake) = waiter.replace(target, context.completion()) else {
            return Ok(());
        };
        if self.len.get() >= self.capacity.get() {
            return Err(Error::QueueFull {
                capacity: self.capacity,
                wake,
            });
        }
        let (previous, vacant) = waiter.vacate();
        vacant.enqueue(self, wake);
        if let Some(previous) = previous {
            previous.wake();
        }
        Ok(())
    }

    pub fn wake(&self, work: app::Work<'_, 'd>) -> WakeStatus {
        loop {
            match work.run(|| self.pop()) {
                quota::Admission::Item(wake) => wake.wake(),
                quota::Admission::Exhausted if self.len.get() != 0 => return WakeStatus::Pending,
                quota::Admission::Empty | quota::Admission::Exhausted => {
                    return WakeStatus::Complete;
                }
            }
        }
    }

    /// Releases every registered waiter when its owner becomes terminal.
    pub fn wake_all(&self) {
        while let Some(wake) = self.pop() {
            wake.wake();
        }
    }

    pub fn wake_one(&self, work: app::Work<'_, 'd>) -> WakeStatus {
        match work.run(|| self.pop()) {
            quota::Admission::Item(wake) => {
                wake.wake();
                WakeStatus::Complete
            }
            quota::Admission::Exhausted if self.len.get() != 0 => WakeStatus::Pending,
            quota::Admission::Empty | quota::Admission::Exhausted => WakeStatus::Complete,
        }
    }

    pub fn len(&self) -> Length {
        Length::new(self.len.get())
    }

    fn detach(&self, entry: Pin<&depot::raw::Entry<cell::Cell<Option<Active<'d>>>>>) {
        // SAFETY: the active registration identifies this exact linked queue.
        unsafe { self.links.remove(entry) };
        self.len.set(self.len.get() - 1);
    }

    fn pop(&self) -> Option<wake::Waker<'d>> {
        let entry = self.links.front()?;
        let active = entry.value().take();
        self.detach(entry);
        active.map(|active| active.wake)
    }
}

/// A one-word endpoint containing at most one caller-owned waiter.
///
/// ```compile_fail
/// use std::pin::{Pin, pin};
/// use dope_fiber::{context::Context, wait::{Slot, Waiter}};
///
/// fn target_cannot_drop_first<'poll, 'd>(context: Pin<&Context<'poll, 'd>>) {
///     let waiter = pin!(Waiter::new());
///     {
///         let slot = Slot::new();
///         assert!(slot.try_register(waiter.as_ref(), context).is_ok());
///     }
///     waiter.as_ref().unregister();
/// }
/// ```
#[repr(align(2))]
pub struct Slot<'d> {
    waiter: cell::Cell<Option<Link<'d>>>,
    thread: marker::PhantomData<ThreadBound>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct Link<'d>(ptr::NonNull<depot::raw::Entry<cell::Cell<Option<Active<'d>>>>>);

impl<'d> Link<'d> {
    fn entry(&self) -> Pin<&depot::raw::Entry<cell::Cell<Option<Active<'d>>>>> {
        // SAFETY: an occupied slot retains its pinned waiter until it unlinks itself.
        unsafe { Pin::new_unchecked(self.0.as_ref()) }
    }
}

impl<'d> Slot<'d> {
    pub const fn new() -> Self {
        Self {
            waiter: cell::Cell::new(None),
            thread: marker::PhantomData,
        }
    }

    pub fn try_register<'target, 'poll>(
        &'target self,
        waiter: Pin<&Waiter<'target, 'd>>,
        context: Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), Error<'d>> {
        let target = Target::slot(self);
        let Some(wake) = waiter.replace(target, context.completion()) else {
            return Ok(());
        };
        if self.waiter.get().is_some() {
            return Err(Error::SlotFull(wake));
        }
        let (previous, vacant) = waiter.vacate();
        vacant.bind(self, wake);
        if let Some(previous) = previous {
            previous.wake();
        }
        Ok(())
    }

    fn take(&self) -> Option<wake::Waker<'d>> {
        let link = self.waiter.take()?;
        let active = link.entry().value().take();
        active.map(|active| active.wake)
    }

    pub fn wake_one(&self, work: app::Work<'_, 'd>) -> WakeStatus {
        match work.run(|| self.take()) {
            quota::Admission::Item(wake) => {
                wake.wake();
                WakeStatus::Complete
            }
            quota::Admission::Exhausted if self.waiter.get().is_some() => WakeStatus::Pending,
            quota::Admission::Empty | quota::Admission::Exhausted => WakeStatus::Complete,
        }
    }

    pub fn wake(&self) {
        if let Some(wake) = self.take() {
            wake.wake();
        }
    }
}

impl Default for Slot<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// A caller-owned pinned registration; its invariant target borrow outlives unlinking.
///
/// ```compile_fail
/// use dope_fiber::wait::Waiter;
/// fn widen<'a, 'b: 'a, 'd>(waiter: Waiter<'a, 'd>) -> Waiter<'b, 'd> { waiter }
/// ```
/// ```compile_fail
/// use dope_fiber::wait::Waiter;
/// fn shorten<'a, 'b: 'a, 'd>(waiter: Waiter<'b, 'd>) -> Waiter<'a, 'd> { waiter }
/// ```
/// ```compile_fail
/// use dope_fiber::wait::Waiter;
/// fn widen<'target, 'a, 'b: 'a>(waiter: Waiter<'target, 'a>) -> Waiter<'target, 'b> { waiter }
/// ```
/// ```compile_fail
/// use dope_fiber::wait::Waiter;
/// fn shorten<'target, 'a, 'b: 'a>(waiter: Waiter<'target, 'b>) -> Waiter<'target, 'a> { waiter }
/// ```
#[pin_project::pin_project(PinnedDrop)]
#[repr(C)]
pub struct Waiter<'target, 'd> {
    #[pin]
    entry: depot::raw::Entry<cell::Cell<Option<Active<'d>>>>,
    thread: ThreadBound,
    target: marker::PhantomData<cell::Cell<&'target ()>>,
}

struct Vacant<'a, 'target, 'd> {
    waiter: Pin<&'a Waiter<'target, 'd>>,
}

impl<'target, 'd> Vacant<'_, 'target, 'd> {
    fn enqueue(self, queue: &'target Queue<'d>, wake: wake::Waker<'d>) {
        self.waiter.entry.value().set(Some(Active {
            target: Target::queue(queue),
            wake,
        }));
        // SAFETY: vacating detached this pinned entry; the invariant target borrow
        // and Waiter::drop keep it live until it removes itself from this queue.
        unsafe { queue.links.push_back(self.waiter.entry()) };
        queue.len.set(queue.len.get() + 1);
    }

    fn bind(self, slot: &'target Slot<'d>, wake: wake::Waker<'d>) {
        self.waiter.entry.value().set(Some(Active {
            target: Target::slot(slot),
            wake,
        }));
        slot.waiter.set(Some(Link(ptr::NonNull::from(
            self.waiter.entry().get_ref(),
        ))));
    }
}

impl Default for Waiter<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'target, 'd> Waiter<'target, 'd> {
    pub const fn new() -> Self {
        Self {
            entry: depot::raw::Entry::new(cell::Cell::new(None)),
            thread: ThreadBound::NEW,
            target: marker::PhantomData,
        }
    }

    fn entry(self: Pin<&Self>) -> Pin<&depot::raw::Entry<cell::Cell<Option<Active<'d>>>>> {
        self.project_ref().entry
    }

    fn replace(&self, target: Target<'d>, wake: wake::Waker<'d>) -> Option<wake::Waker<'d>> {
        match self.entry.value().take() {
            Some(previous) if previous.target == target => {
                let changed = previous.wake != wake;
                self.entry.value().set(Some(Active { target, wake }));
                if changed {
                    previous.wake.wake();
                }
                None
            }
            previous => {
                self.entry.value().set(previous);
                Some(wake)
            }
        }
    }

    fn vacate<'a>(self: Pin<&'a Self>) -> (Option<wake::Waker<'d>>, Vacant<'a, 'target, 'd>) {
        let Some(active) = self.entry.value().take() else {
            return (None, Vacant { waiter: self });
        };
        self.unlink(&active);
        (Some(active.wake), Vacant { waiter: self })
    }

    fn unlink<'a>(self: Pin<&'a Self>, active: &Active<'d>) {
        let tagged = active.target.pointer;
        let pointer = tagged.map_addr(|addr| {
            // SAFETY: removing the tag from a live aligned endpoint cannot produce zero.
            unsafe { num::NonZero::<usize>::new_unchecked(addr.get() & !1) }
        });
        if tagged.addr().get() & 1 == 0 {
            // SAFETY: this pinned waiter retains its queue's invariant target borrow.
            let queue: &'a Queue<'d> = unsafe { pointer.cast::<Queue<'d>>().as_ref() };
            queue.detach(self.entry());
        } else {
            // SAFETY: the same target borrow keeps this exact slot live and unmoved.
            let slot: &'a Slot<'d> = unsafe { pointer.cast::<Slot<'d>>().as_ref() };
            slot.waiter.set(None);
        }
    }

    pub fn unregister(self: Pin<&Self>) {
        let _ = self.vacate();
    }

    pub fn registration(&self) -> Registration {
        // SAFETY: this shared observation neither mutates the cell nor calls user code.
        match unsafe { &*self.entry.value().as_ptr() } {
            Some(_) => Registration::Active,
            None => Registration::Vacant,
        }
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Waiter<'_, '_> {
    fn drop(self: Pin<&mut Self>) {
        self.as_ref().unregister();
    }
}

const _: () = {
    assert!(mem::align_of::<Queue<'static>>() >= 2);
    assert!(mem::align_of::<Slot<'static>>() >= 2);
    assert!(mem::size_of::<Option<Active<'static>>>() == 3 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<wake::Waker<'static>>>() == mem::size_of::<wake::Waker<'static>>()
    );
    assert!(mem::size_of::<Option<Link<'static>>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Queue<'static>>() == 5 * mem::size_of::<usize>());
    assert!(mem::size_of::<Slot<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Waiter<'static, 'static>>() == 5 * mem::size_of::<usize>());
};
