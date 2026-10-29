use std::{mem, ptr};

use dope_sys::thread;

use crate::platform::thread::{Finished, Slot, Task};

pub(super) struct Group<T, O, P> {
    pub(super) slots: Vec<Slot<T, O, P>>,
    /// Successful spawn prefix; this owner retains every remaining input.
    pub(super) started: usize,
}

impl<T, O, P> Drop for Group<T, O, P> {
    fn drop(&mut self) {
        self.take();
    }
}

impl<T, O, P> Group<T, O, P> {
    pub(super) fn parents_mut(&mut self) -> impl ExactSizeIterator<Item = &mut P> {
        let slots = self.slots.as_mut_ptr();
        (0..self.started).map(move |index| {
            // SAFETY: the started prefix lies within this fixed allocation.
            // Native entries access only task/output cells; this projection
            // lends only disjoint coordinator metadata for the Group borrow.
            unsafe {
                let slot = slots.add(index);
                &mut *ptr::addr_of_mut!((*slot).parent)
            }
        })
    }

    /// Joins every worker, drops unstarted inputs, and transfers the output prefix.
    pub(super) fn take(&mut self) -> Finished<T, O, P> {
        // SAFETY: successful spawns alone extend started within this fixed
        // allocation; Workers cannot grow or remove slots before this take.
        for slot in unsafe { self.slots.get_unchecked(..self.started) } {
            // SAFETY: start stores each handle before extending this prefix.
            // This take consumes each handle and then clears the prefix before
            // any later drop; Handle retains thread confinement.
            let handle = unsafe {
                slot.thread
                    .replace(mem::MaybeUninit::uninit())
                    .assume_init()
            };
            handle.join();
        }
        let len = self.slots.len();
        let slots = self.slots.as_mut_ptr();
        // SAFETY: started fits this fixed allocation. The detached suffix
        // stays initialized and exclusively owned here until consumed below.
        unsafe { self.slots.set_len(self.started) };
        for index in self.started..len {
            // SAFETY: every original suffix slot is consumed exactly once.
            let mut slot = unsafe { slots.add(index).read() };
            // SAFETY: only a successful spawn transfers an input out of this
            // suffix. Builder and Group retain every other initialized input.
            unsafe { mem::ManuallyDrop::drop(&mut slot.value.get_mut().task) };
        }
        self.started = 0;
        Finished {
            slots: mem::take(&mut self.slots).into_iter(),
        }
    }
}

// SAFETY: the native entry accesses only this slot's task and output cells;
// its Group retains their exclusive worker access until the stored handle joins.
unsafe impl<T: Task<Output = O>, O, P> thread::raw::Task for Slot<T, O, P> {
    /// Runs this slot's task once and stores its output. `Workers::start` admits
    /// each slot once and stores its handle before its index advances, so the
    /// retained task cell holds exactly one task at this point.
    unsafe fn run(context: ptr::NonNull<Self>) {
        let slot = context.as_ptr();
        // SAFETY: the admitted context grants this invocation sole access to
        // task and output. The consumed native entry transfers the initialized
        // input once; Group only drops inputs outside its successful prefix.
        let task = unsafe { mem::ManuallyDrop::take(&mut (*(*slot).value.get()).task) };
        let output = task.run();
        // SAFETY: the same retained context grants exclusive output access.
        unsafe { (*(*slot).value.get()).output = mem::ManuallyDrop::new(output) };
    }
}
