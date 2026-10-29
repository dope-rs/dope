use core::{mem, mem::ManuallyDrop, pin, task::Poll};

use crate::{
    context,
    task::storage::slots::{self, Action, Phase, State},
};

union Value<F, O> {
    vacant: (),
    fiber: ManuallyDrop<F>,
    output: ManuallyDrop<O>,
}

impl<F, O> Value<F, O> {
    /// # Safety
    /// The fiber field must be the union's initialized value.
    unsafe fn drop_fiber(&mut self) {
        // SAFETY: the caller selects the initialized fiber; it is not moved.
        unsafe { ManuallyDrop::drop(&mut self.fiber) };
    }
}

/// One pinned task value whose phase selects its sole initialized union field.
pub(crate) struct Entry<F, O> {
    state: State,
    value: Value<F, O>,
}

/// Exclusive proof that one task slot is in its terminal output phase.
#[repr(transparent)]
pub(crate) struct Done<'a, F, O> {
    slot: pin::Pin<&'a mut Entry<F, O>>,
}

impl<'a, F, O> Done<'a, F, O> {
    pub(super) fn new(slot: pin::Pin<&'a mut Entry<F, O>>) -> Option<Self> {
        if slot.state() != State::Staged {
            return None;
        }
        // SAFETY: the phase check proves output is the initialized union field.
        Some(unsafe { Self::exact(slot) })
    }

    /// # Safety
    /// This exact slot must contain its initialized terminal output.
    pub(in crate::task) unsafe fn exact(slot: pin::Pin<&'a mut Entry<F, O>>) -> Self {
        Self { slot }
    }

    pub(crate) fn take(self) -> O {
        // SAFETY: Done retains the exclusive pinned slot; its output is unpinned.
        let slot = unsafe { self.slot.get_unchecked_mut() };
        // SAFETY: construction proved output is the initialized union field.
        let output = unsafe { ManuallyDrop::take(&mut slot.value.output) };
        // SAFETY: output was moved above and Vacant has no live union field.
        slot.value.vacant = ();
        slot.state = State::Vacant;
        output
    }
}

impl<F, O> Entry<F, O> {
    pub(in crate::task) fn poll_fiber<'a, 'd>(
        self: pin::Pin<&'a mut Self>,
        mut context: pin::Pin<&mut crate::context::Context<'_, 'd>>,
    ) -> Option<Poll<(Done<'a, F, O>, bool)>>
    where
        F: crate::abi::Fiber<'d, Output = O>,
    {
        self.poll(|fiber| {
            // SAFETY: Entry admits only its Live arm and replaces it with Done
            // before returning Ready; the same fiber cannot be admitted again.
            unsafe { context::raw::Live::new(fiber).poll(context.as_mut()) }
        })
    }

    /// Polls once and retains the exact completed slot until its output is used.
    pub(crate) fn poll(
        mut self: pin::Pin<&mut Self>,
        run: impl FnOnce(pin::Pin<&mut F>) -> Poll<O>,
    ) -> Option<Poll<(Done<'_, F, O>, bool)>> {
        let fresh = match self.as_mut().visit((), |fiber, ()| match run(fiber) {
            Poll::Pending => Action::Keep(false),
            Poll::Ready(output) => Action::Complete(output, true),
        }) {
            Phase::Vacant(()) => return None,
            Phase::Live(false) => return Some(Poll::Pending),
            Phase::Live(true) => true,
            Phase::Done(()) => false,
        };
        // SAFETY: the retained exclusive slot either just completed or was
        // already Done; no mutation intervenes before this capability is issued.
        Some(Poll::Ready((unsafe { Done::exact(self) }, fresh)))
    }

    pub(crate) const fn vacant() -> Self {
        Self {
            state: State::Vacant,
            value: Value { vacant: () },
        }
    }

    pub(crate) const fn live(fiber: F) -> Self {
        Self {
            state: State::Live,
            value: Value {
                fiber: ManuallyDrop::new(fiber),
            },
        }
    }

    pub(crate) fn visit<I, R>(
        self: pin::Pin<&mut Self>,
        input: I,
        run: impl for<'b> FnOnce(pin::Pin<&'b mut F>, I) -> Action<O, R>,
    ) -> Phase<I, R> {
        let state = self.state();
        if state == State::Vacant {
            return Phase::Vacant(input);
        }
        if state == State::Staged {
            return Phase::Done(input);
        }
        // SAFETY: Live selects fiber, and the enclosing Slot pin pins this field.
        let this = unsafe { self.get_unchecked_mut() };
        let fiber = unsafe { pin::Pin::new_unchecked(&mut *this.value.fiber) };
        match run(fiber, input) {
            Action::Keep(result) => Phase::Live(result),
            Action::Complete(output, result) => {
                // SAFETY: Live selects this initialized pinned fiber.
                unsafe { this.value.drop_fiber() };
                this.value.output = ManuallyDrop::new(output);
                this.state = State::Staged;
                Phase::Live(result)
            }
            Action::Vacate(result) => {
                // SAFETY: the phase check retains this exact live pinned slot.
                unsafe { this.vacate() };
                Phase::Live(result)
            }
        }
    }

    pub(super) fn cancel(self: pin::Pin<&mut Self>, before: impl FnOnce()) {
        if self.state() != State::Live {
            return;
        }
        before();
        // SAFETY: the exclusive pin retains this live slot while its binding
        // is reclaimed, and vacate destroys the fiber without moving it.
        unsafe { self.get_unchecked_mut().vacate() };
    }

    /// # Safety
    /// The exclusive owner must retain this slot's live, pinned fiber.
    unsafe fn vacate(&mut self) {
        // SAFETY: the caller retains the initialized pinned fiber.
        unsafe { self.value.drop_fiber() };
        self.value.vacant = ();
        self.state = State::Vacant;
    }

    pub(crate) const fn state(&self) -> State {
        self.state
    }

    pub(crate) fn done(self: pin::Pin<&mut Self>) -> Option<Done<'_, F, O>> {
        Done::new(self)
    }
}

impl<F, O> Drop for Entry<F, O> {
    fn drop(&mut self) {
        match self.state {
            State::Vacant => {}
            // SAFETY: Live selects the initialized fiber, which is not moved.
            State::Live => unsafe { self.value.drop_fiber() },
            // SAFETY: Staged selects the initialized output.
            State::Staged => unsafe { ManuallyDrop::drop(&mut self.value.output) },
        }
    }
}

/// Exclusive access to one entry with no initialized fiber or output.
#[repr(transparent)]
pub(in crate::task) struct Vacant<'a, F, O> {
    slot: &'a mut Entry<F, O>,
}

impl<'a, F, O> Vacant<'a, F, O> {
    /// # Safety
    /// This exact table entry must have no initialized fiber or output;
    /// its bounded index alone does not prove that phase.
    pub(in crate::task) unsafe fn new<'d, const N: usize>(
        table: &'a mut slots::Table<'d, F, O, N>,
        index: slots::Index<N>,
    ) -> Self {
        // SAFETY: Index<N> is in bounds; the enclosing owner proves vacancy.
        // This capability retains the exact exclusive entry until consumption.
        let slot = unsafe { table.slots.get_unchecked_mut(index.get()) };
        Self { slot }
    }

    /// # Safety
    /// This exact pinned entry must have no initialized fiber or output.
    pub(in crate::task) unsafe fn pinned<'d, const N: usize>(
        table: pin::Pin<&'a mut slots::Table<'d, F, O, N>>,
        index: slots::Index<N>,
    ) -> Self {
        // SAFETY: this capability can only initialize the vacant entry;
        // it cannot move the table or any existing pinned fiber.
        unsafe { Self::new(table.get_unchecked_mut(), index) }
    }

    pub(in crate::task) fn insert(self, fiber: F) {
        self.slot.value.fiber = ManuallyDrop::new(fiber);
        self.slot.state = slots::State::Live;
    }
}

const _: () = {
    assert!(mem::size_of::<Vacant<'static, (), ()>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Vacant<'static, (), ()>>>() == mem::size_of::<usize>());
};
