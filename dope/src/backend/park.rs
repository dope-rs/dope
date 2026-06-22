use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomPinned;
use std::ptr::NonNull;
use std::task::{RawWaker, RawWakerVTable, Waker};

use crate::backend::token::{Epoch, LocalIdx, Token};

const _: () = assert!(size_of::<Token>() == size_of::<*const ()>());

pub struct Slot {
    target: Cell<Token>,
    queued: Cell<bool>,
    arena: NonNull<Arena>,
}

impl Slot {
    pub(super) fn new(target: Token, arena: NonNull<Arena>) -> Self {
        Self {
            target: Cell::new(target),
            queued: Cell::new(false),
            arena,
        }
    }

    pub fn set_target(&self, target: Token) {
        self.target.set(target);
    }

    pub fn make_waker(&self) -> Waker {
        // SAFETY: VTABLE reads data via from_data; the park::Slot outlives the Waker.
        unsafe { Waker::from_raw(self.raw_waker()) }
    }

    pub fn wake_ref(&self) -> WakeRef {
        WakeRef(NonNull::from(self))
    }

    pub fn wake(&self) {
        if self.queued.replace(true) {
            return;
        }
        // SAFETY: arena is set to the owning park::Arena at construction, live for the runtime; thread-per-core invariant — wake() and drain() never alias the live queue.
        let arena = unsafe { self.arena.as_ref() };
        unsafe {
            (*arena.live.get()).push(NonNull::from(self));
        }
    }

    fn raw_waker(&self) -> RawWaker {
        RawWaker::new(self as *const Slot as *const (), &VTABLE)
    }

    unsafe fn from_data<'a>(data: *const ()) -> &'a Slot {
        // SAFETY: data is a park::Slot pointer minted by raw_waker; the referent outlives the Waker.
        unsafe { &*(data as *const Slot) }
    }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake, vt_drop);

// SAFETY: std RawWakerVTable ABI — data is a park::Slot pointer; clone re-wraps the same data.
unsafe fn vt_clone(data: *const ()) -> RawWaker {
    unsafe { Slot::from_data(data) }.raw_waker()
}

// SAFETY: std RawWakerVTable ABI — data is a park::Slot pointer minted by raw_waker.
unsafe fn vt_wake(data: *const ()) {
    unsafe { Slot::from_data(data) }.wake();
}

// SAFETY: std RawWakerVTable ABI — park::Slot is borrowed, not owned, so drop is a no-op.
unsafe fn vt_drop(_data: *const ()) {}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct WakeRef(NonNull<Slot>);

impl WakeRef {
    pub fn verified(w: &Waker) -> Self {
        assert!(
            std::ptr::eq(w.vtable(), &VTABLE),
            "dope: waker not minted by dope's parker"
        );
        // SAFETY: vtable identity checked above, so data is a live Slot at a stable Arena address for the runtime's lifetime.
        Self(unsafe { NonNull::new_unchecked(w.data() as *mut Slot) })
    }

    pub fn wake(&self) {
        // SAFETY: WakeRef wraps a live Slot at a stable Arena address (verified vtable or Slot::wake_ref); thread-per-core.
        unsafe { self.0.as_ref().wake() }
    }
}

pub trait Parker {
    fn slot(&self, slot: crate::backend::socket::FdSlot) -> &Slot;
    fn make_slot(&self, target: Token) -> Slot;
    fn drain(&self, out: &mut Vec<Token>);
    fn is_empty(&self) -> bool;
}

#[repr(C)]
pub(super) struct Arena {
    live: UnsafeCell<Vec<NonNull<Slot>>>,
    _pin: PhantomPinned,
    slots: [Slot],
}

#[repr(C)]
struct ArenaHeader {
    live: UnsafeCell<Vec<NonNull<Slot>>>,
    _pin: PhantomPinned,
}

impl Arena {
    pub(super) fn new(slots: usize) -> std::io::Result<Box<Self>> {
        use std::alloc::{Layout, alloc, handle_alloc_error};
        use std::io;

        let layout_err =
            |_| io::Error::new(io::ErrorKind::InvalidInput, "park::Arena layout overflow");
        let header = Layout::new::<ArenaHeader>();
        let tail = Layout::array::<Slot>(slots).map_err(layout_err)?;
        let (combined, slots_offset) = header.extend(tail).map_err(layout_err)?;
        let final_layout = combined.pad_to_align();

        // SAFETY: single allocation sized for ArenaHeader + [Slot; slots]; initialize each field then construct fat-pointer Box<Arena>.
        let boxed = unsafe {
            let raw = alloc(final_layout);
            if raw.is_null() {
                handle_alloc_error(final_layout);
            }
            std::ptr::write(
                raw as *mut UnsafeCell<Vec<NonNull<Slot>>>,
                UnsafeCell::new(Vec::with_capacity(slots)),
            );
            let fat: *mut Arena =
                std::ptr::slice_from_raw_parts_mut(raw as *mut Slot, slots) as *mut Arena;
            let arena_ptr = NonNull::new_unchecked(fat);
            let dummy = Token::new(0, LocalIdx::new(0), Epoch::INITIAL);
            let slots_start = raw.add(slots_offset) as *mut Slot;
            for i in 0..slots {
                std::ptr::write(slots_start.add(i), Slot::new(dummy, arena_ptr));
            }
            Box::from_raw(fat)
        };
        Ok(boxed)
    }

    pub(super) fn slot(&self, slot: crate::backend::socket::FdSlot) -> &Slot {
        &self.slots[slot.raw() as usize]
    }

    pub(super) fn is_empty(&self) -> bool {
        // SAFETY: thread-per-core invariant — wake()/drain() never alias the live queue.
        unsafe { (*self.live.get()).is_empty() }
    }

    pub(super) fn drain(&self, out: &mut Vec<Token>) {
        // SAFETY: thread-per-core invariant — wake()/drain() never alias the live queue.
        let live = unsafe { &mut *self.live.get() };
        for slot_ptr in live.drain(..) {
            // SAFETY: slot was enqueued by wake(); park::Slot storage lives for the runtime.
            let slot = unsafe { slot_ptr.as_ref() };
            slot.queued.set(false);
            out.push(slot.target.get());
        }
    }
}
