use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::Fiber;

struct Slot<'d, F: Future> {
    cell: MaybeUninit<Fiber<'d, F>>,
}

impl<'d, F: Future> Slot<'d, F> {
    const fn vacant() -> Self {
        Self {
            cell: MaybeUninit::uninit(),
        }
    }
}

struct Vacant<'s, 'd, F: Future> {
    slot: Pin<&'s mut Slot<'d, F>>,
}

struct Filled<'s, 'd, F: Future> {
    slot: Pin<&'s mut Slot<'d, F>>,
}

impl<'s, 'd, F: Future> Vacant<'s, 'd, F> {
    fn fill(self, fiber: Fiber<'d, F>) -> Filled<'s, 'd, F> {
        let mut slot = self.slot;
        // SAFETY: writing into a vacant cell moves no pinned value; the Fiber stays pinned in place thereafter.
        unsafe { slot.as_mut().get_unchecked_mut().cell.write(fiber) };
        Filled { slot }
    }
}

impl<'s, 'd, F: Future> Filled<'s, 'd, F> {
    unsafe fn assume(slot: Pin<&'s mut Slot<'d, F>>) -> Self {
        // SAFETY: caller guarantees the slot's cell holds an initialized Fiber.
        Self { slot }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: the `Filled` typestate proves the cell holds an initialized, structurally pinned Fiber.
        unsafe {
            let fiber = &mut *self.slot.as_mut().get_unchecked_mut().cell.as_mut_ptr();
            Pin::new_unchecked(fiber).poll(cx)
        }
    }

    fn clear(self) -> Vacant<'s, 'd, F> {
        let mut slot = self.slot;
        // SAFETY: the `Filled` typestate proves the cell holds an initialized, droppable Fiber.
        unsafe { slot.as_mut().get_unchecked_mut().cell.assume_init_drop() };
        Vacant { slot }
    }
}

pub struct TaskId(u32);

pub struct Slab<'d, F, const N: usize>
where
    F: Future + 'd,
{
    slots: Pin<Box<[Slot<'d, F>; N]>>,
    free: Box<[u32; N]>,
    free_len: usize,
}

impl<'d, F, const N: usize> Default for Slab<'d, F, N>
where
    F: Future + 'd,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, F, const N: usize> Slab<'d, F, N>
where
    F: Future + 'd,
{
    pub fn new() -> Self {
        const {
            assert!(N > 0, "fiber slab capacity must be > 0");
            assert!(
                N <= u32::MAX as usize,
                "fiber slab capacity must be <= u32::MAX"
            );
        }
        let slots: Box<[Slot<'d, F>; N]> = Vec::from_iter((0..N).map(|_| Slot::vacant()))
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| unreachable!("slots length is exactly N"));
        let free: Box<[u32; N]> = Vec::from_iter(0..N as u32)
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| unreachable!("free length is exactly N"));
        Self {
            slots: Box::into_pin(slots),
            free,
            free_len: N,
        }
    }

    fn project(&mut self, idx: u32) -> Pin<&mut Slot<'d, F>> {
        // SAFETY: structural pin projection of an array element — each Slot is pinned for the slab's life and never moves.
        unsafe {
            let slots = self.slots.as_mut().get_unchecked_mut();
            Pin::new_unchecked(&mut slots[idx as usize])
        }
    }

    pub fn alloc(&mut self, fiber: Fiber<'d, F>) -> Option<TaskId> {
        if self.free_len == 0 {
            return None;
        }
        self.free_len -= 1;
        let idx = self.free[self.free_len];
        Vacant {
            slot: self.project(idx),
        }
        .fill(fiber);
        Some(TaskId(idx))
    }

    pub fn poll(&mut self, id: &TaskId, cx: &mut Context<'_>) -> Poll<F::Output> {
        let slot = self.project(id.0);
        // SAFETY: a live `TaskId` is proof that slot `id.0` is filled.
        let mut filled = unsafe { Filled::assume(slot) };
        filled.poll(cx)
    }

    pub fn release(&mut self, id: TaskId) {
        let slot = self.project(id.0);
        // SAFETY: a live `TaskId` is proof that slot `id.0` is filled; consuming it bars any reuse.
        let filled = unsafe { Filled::assume(slot) };
        filled.clear();
        self.free[self.free_len] = id.0;
        self.free_len += 1;
    }
}

impl<'d, F, const N: usize> Drop for Slab<'d, F, N>
where
    F: Future + 'd,
{
    fn drop(&mut self) {
        self.free[..self.free_len].sort_unstable();
        let mut next_free = 0usize;
        for idx in 0..N as u32 {
            if next_free < self.free_len && self.free[next_free] == idx {
                next_free += 1;
                continue;
            }
            let slot = self.project(idx);
            // SAFETY: slot `idx` is absent from the free-list, so a `TaskId` for it was issued and never released — the cell is filled.
            let filled = unsafe { Filled::assume(slot) };
            filled.clear();
        }
    }
}
