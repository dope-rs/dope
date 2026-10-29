use core::{cell, marker, mem, pin, ptr};

use o3::depot::{
    arena::slab::key::{
        Generation, Parts,
        cycle::{self, Policy as _},
    },
    queue::{indexed::keyed, links},
};
use set::ready;
use state::State;

use crate::{
    abi,
    context::wake,
    core::driver::schedule::ready::task,
    task::{
        storage,
        storage::{Handle, set},
    },
};

mod done;
pub(in crate::task::storage::set) use done::{Done, Listed};

pub(in crate::task::storage::set) mod raw;
pub(in crate::task::storage) mod state;

#[repr(transparent)]
pub(in crate::task::storage::set) struct Admitting<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()>(
    pin::Pin<&'a Slot<'d, F, M, D, G>>,
);

#[repr(transparent)]
pub(in crate::task::storage::set) struct Admitted<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()>(
    pin::Pin<&'a Slot<'d, F, M, D, G>>,
);

#[repr(transparent)]
pub(in crate::task::storage::set) struct Counted<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()>(
    Admitted<'a, 'd, F, M, D, G>,
);

#[repr(transparent)]
pub(in crate::task::storage::set) struct Vacated<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()>(
    pin::Pin<&'a Slot<'d, F, M, D, G>>,
);

pub(in crate::task::storage::set) struct Running<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()>(
    pin::Pin<&'a Slot<'d, F, M, D, G>>,
);

pub(in crate::task::storage) struct Discardable<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    slot: pin::Pin<&'a Slot<'d, F, M, D, G>>,
    phase: DiscardPhase,
}

enum DiscardPhase {
    Fiber,
    Output,
}

/// Caller-owned pinned slots use external keyed order FIFOs without a per-lane FIFO plane.
#[pin_project::pin_project]
pub(in crate::task::storage) struct Slot<'d, F: abi::Fiber<'d>, M, D, G = ()> {
    #[pin]
    value: Value<'d, F, M, D, G>,
    generation: cell::Cell<Generation>,
    pub(in crate::task::storage::set) link: keyed::Link<Handle<'d, D>, storage::Id<'d, D>>,
    pub(in crate::task::storage::set) free: cell::Cell<ready::raw::Index<D>>,
    domain: marker::PhantomData<fn(D) -> D>,
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Slot<'d, F, M, D, G> {
    pub(super) fn new(index: ready::raw::Index<D>, next: Option<ready::raw::Index<D>>) -> Self {
        Self {
            value: Value::uninit(),
            generation: cell::Cell::new(Generation::MIN),
            link: keyed::Link::from(links::Index::from(index.position())),
            free: cell::Cell::new(next.unwrap_or(index)),
            domain: marker::PhantomData,
        }
    }

    pub(in crate::task::storage) fn state(&self) -> State {
        self.value.state()
    }

    pub(in crate::task::storage) fn matches(&self, parts: Parts) -> bool {
        self.state().is_occupied() && self.generation.get() == parts.generation()
    }

    pub(in crate::task::storage) fn current(&self, index: u32) -> Handle<'d, D> {
        Handle::from_parts(Parts::new(index, self.generation.get()))
    }

    pub(in crate::task::storage) fn resident<'a>(
        self: pin::Pin<&'a Self>,
        token: &'a mut Token<'_, F, M>,
    ) -> Option<pin::Pin<&'a mut F>> {
        match self.state() {
            State::Running | State::Queued => Some(token.fiber(self.project_ref().value)),
            State::Free
            | State::Admitting
            | State::Completed
            | State::External
            | State::Retiring
            | State::Retired => None,
        }
    }

    pub(in crate::task::storage::set) fn begin(
        self: pin::Pin<&Self>,
    ) -> Admitting<'_, 'd, F, M, D, G> {
        Admitting(self)
    }

    pub(in crate::task::storage::set) fn data(self: pin::Pin<&Self>) -> &M {
        self.project_ref().value.data()
    }

    fn externalize(self: pin::Pin<&Self>, group: G)
    where
        G: Copy,
    {
        self.project_ref().value.externalize(group);
    }

    pub(in crate::task::storage::set) fn take_group(self: pin::Pin<&Self>) -> G
    where
        G: Copy,
    {
        self.project_ref().value.take_group()
    }

    pub(in crate::task::storage) fn discardable(
        self: pin::Pin<&Self>,
    ) -> Option<Discardable<'_, 'd, F, M, D, G>> {
        let phase = match self.state() {
            State::Admitting | State::Running | State::Queued => DiscardPhase::Fiber,
            State::Completed | State::External => DiscardPhase::Output,
            State::Free | State::Retired | State::Retiring => return None,
        };
        Some(Discardable { slot: self, phase })
    }

    pub(in crate::task::storage::set) fn try_wake(
        self: pin::Pin<&Self>,
        key: Handle<'d, D>,
    ) -> Result<(), set::WakeError> {
        if !self.matches(key.parts()) {
            return Err(set::WakeError::Missing);
        }
        match self.state() {
            State::Running => self.project_ref().value.root().poll().1.wake(),
            State::Queued | State::Completed | State::External => {}
            State::Free | State::Admitting | State::Retiring | State::Retired => {
                return Err(set::WakeError::Inactive);
            }
        }
        Ok(())
    }

    pub(in crate::task::storage::set) fn queue_wake(&self) {
        self.value.transition(State::Queued);
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Admitting<'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn write(
        self,
        token: &mut Token<'_, F, M>,
        root: task::raw::Root<'d>,
        fiber: F,
        data: M,
    ) -> Admitted<'a, 'd, F, M, D, G> {
        token.write(self.0.project_ref().value, root, fiber, data);
        Admitted(self.0)
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Counted<'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) const fn new(admitted: Admitted<'a, 'd, F, M, D, G>) -> Self {
        Self(admitted)
    }

    pub(in crate::task::storage::set) fn slot(&self) -> pin::Pin<&'a Slot<'d, F, M, D, G>> {
        self.0.0
    }

    pub(in crate::task::storage::set) fn queue(self) {
        self.0.0.value.transition(State::Queued);
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Vacated<'a, 'd, F, M, D, G> {
    const fn new(slot: pin::Pin<&'a Slot<'d, F, M, D, G>>) -> Self {
        Self(slot)
    }

    pub(in crate::task::storage::set) fn recycle(
        self,
        head: &cell::Cell<Option<ready::raw::Index<D>>>,
    ) {
        let slot = self.0;
        let Ok(next) = cycle::Mode::<false>::next(slot.generation.get()) else {
            slot.value.transition(State::Retired);
            return;
        };
        slot.generation.set(next);
        slot.value.transition(State::Free);
        let index = slot.free.get();
        slot.free.set(head.get().unwrap_or(index));
        head.set(Some(index));
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Running<'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn data(&self) -> &M {
        self.0.project_ref().value.data()
    }

    pub(in crate::task::storage::set) fn wakers(&self) -> (wake::RootWaker<'d>, wake::Waker<'d>) {
        let (parent, wake) = self.0.project_ref().value.root().poll();
        (
            wake::RootWaker::from(parent),
            wake::Waker::from_parent(wake),
        )
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Discardable<'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn discard(
        self,
        token: &mut Token<'_, F, M>,
    ) -> Vacated<'a, 'd, F, M, D, G> {
        match self.phase {
            DiscardPhase::Fiber => {
                token.drop_running(self.slot.project_ref().value);
            }
            DiscardPhase::Output => {
                token.drop_output(self.slot.project_ref().value);
            }
        }
        Vacated::new(self.slot)
    }
}

const _: () = assert!(
    mem::size_of::<Slot<'static, abi::Ready<()>, (), ()>>()
        == mem::size_of::<Value<'static, abi::Ready<()>, (), ()>>() + 2 * mem::size_of::<usize>()
);

pub(in crate::task::storage) struct Token<'a, F, M> {
    unique: marker::PhantomData<&'a mut (F, M)>,
}

union Body<F, O> {
    running: mem::ManuallyDrop<F>,
    output: mem::ManuallyDrop<O>,
}

union Life<'d, G> {
    root: mem::ManuallyDrop<task::raw::Root<'d>>,
    group: mem::ManuallyDrop<G>,
}

struct Fields<'d, F: abi::Fiber<'d>, M, D, G> {
    life: Life<'d, G>,
    data: mem::ManuallyDrop<M>,
    body: Body<F, F::Output>,
    domain: marker::PhantomData<fn(D) -> D>,
}

pub(super) struct Value<'d, F: abi::Fiber<'d>, M, D, G = ()> {
    fields: cell::UnsafeCell<mem::MaybeUninit<Fields<'d, F, M, D, G>>>,
    state: cell::Cell<State>,
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Value<'d, F, M, D, G> {
    const fn uninit() -> Self {
        Self {
            fields: cell::UnsafeCell::new(mem::MaybeUninit::uninit()),
            state: cell::Cell::new(State::Free),
        }
    }

    fn state(&self) -> State {
        self.state.get()
    }

    fn transition(&self, state: State) {
        self.state.set(state);
    }

    fn data(self: pin::Pin<&Self>) -> &M {
        let fields = self.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: occupied owners retain metadata independently of the running fiber.
        unsafe { &*ptr::addr_of!((*fields).data).cast::<M>() }
    }

    fn root(self: pin::Pin<&Self>) -> &task::raw::Root<'d> {
        let fields = self.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: the running/admitted owner retains this live root union arm.
        unsafe { &*ptr::addr_of!((*fields).life.root).cast::<task::raw::Root<'d>>() }
    }

    fn externalize(self: pin::Pin<&Self>, group: G)
    where
        G: Copy,
    {
        let value = self.get_ref();
        let fields = value.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: completed ownership consumed the root before installing its group.
        unsafe { ptr::addr_of_mut!((*fields).life.group).write(mem::ManuallyDrop::new(group)) };
        value.state.set(State::External);
    }

    fn take_group(self: pin::Pin<&Self>) -> G
    where
        G: Copy,
    {
        let fields = self.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: only external ownership reads its initialized Copy group arm.
        unsafe { ptr::addr_of!((*fields).life.group).cast::<G>().read() }
    }
}

impl<F, M> Token<'_, F, M> {
    pub(in crate::task::storage) const fn new() -> Self {
        Self {
            unique: marker::PhantomData,
        }
    }

    fn write<'d, D, G>(
        &mut self,
        value: pin::Pin<&Value<'d, F, M, D, G>>,
        root: task::raw::Root<'d>,
        fiber: F,
        data: M,
    ) where
        F: abi::Fiber<'d>,
    {
        let value = value.get_ref();
        unsafe {
            (&mut *value.fields.get()).write(Fields {
                life: Life {
                    root: mem::ManuallyDrop::new(root),
                },
                data: mem::ManuallyDrop::new(data),
                body: Body {
                    running: mem::ManuallyDrop::new(fiber),
                },
                domain: marker::PhantomData,
            });
        }
        value.state.set(State::Admitting);
    }

    fn fiber<'a, 'd, D, G>(
        &'a mut self,
        value: pin::Pin<&'a Value<'d, F, M, D, G>>,
    ) -> pin::Pin<&'a mut F>
    where
        F: abi::Fiber<'d>,
    {
        let fields = value.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: the caller retains the running phase, and the unique token
        // borrows only that live pinned fiber, independently of metadata.
        let fiber = unsafe { &mut *ptr::addr_of_mut!((*fields).body.running).cast::<F>() };
        unsafe { pin::Pin::new_unchecked(fiber) }
    }

    pub(in crate::task::storage::set) fn running<'a, 'd, D, G>(
        &'a mut self,
        running: &'a Running<'a, 'd, F, M, D, G>,
    ) -> pin::Pin<&'a mut F>
    where
        F: abi::Fiber<'d>,
    {
        self.fiber(running.0.project_ref().value)
    }

    pub(in crate::task::storage::set) fn finish<'d, D, G>(
        &mut self,
        running: Running<'_, 'd, F, M, D, G>,
        output: F::Output,
    ) where
        F: abi::Fiber<'d>,
    {
        let value = running.0.project_ref().value;
        self.retire(value);
        let fields = value.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: retirement destroyed the prior running arm, and the unique
        // token retains this exact value while its output is initialized.
        unsafe {
            ptr::addr_of_mut!((*fields).body.output).write(mem::ManuallyDrop::new(output));
        }
        value.get_ref().state.set(State::Completed);
    }

    fn take<'d, D, G>(&mut self, value: pin::Pin<&Value<'d, F, M, D, G>>) -> (M, F::Output)
    where
        F: abi::Fiber<'d>,
    {
        value.get_ref().state.set(State::Retiring);
        let fields = value.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: completed ownership and the unique token consume both fields once.
        let data = unsafe { ptr::addr_of!((*fields).data).cast::<M>().read() };
        let output = unsafe {
            ptr::addr_of!((*fields).body.output)
                .cast::<F::Output>()
                .read()
        };
        (data, output)
    }

    fn drop_running<'d, D, G>(&mut self, value: pin::Pin<&Value<'d, F, M, D, G>>)
    where
        F: abi::Fiber<'d>,
    {
        self.retire(value);
        let fields = value.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: retirement consumed only root and fiber; this exact token
        // still owns the initialized metadata and destroys it once.
        unsafe { ptr::drop_in_place(ptr::addr_of_mut!((*fields).data).cast::<M>()) };
    }

    fn retire<'d, D, G>(&mut self, value: pin::Pin<&Value<'d, F, M, D, G>>)
    where
        F: abi::Fiber<'d>,
    {
        value.get_ref().state.set(State::Retiring);
        let fields = value.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: the unique token retires these live arms before reuse or reentry.
        unsafe {
            ptr::drop_in_place(
                ptr::addr_of_mut!((*fields).life.root).cast::<task::raw::Root<'d>>(),
            );
            ptr::drop_in_place(ptr::addr_of_mut!((*fields).body.running).cast::<F>());
        }
    }

    fn drop_output<'d, D, G>(&mut self, value: pin::Pin<&Value<'d, F, M, D, G>>)
    where
        F: abi::Fiber<'d>,
    {
        value.get_ref().state.set(State::Retiring);
        let fields = value.fields.get().cast::<Fields<'d, F, M, D, G>>();
        // SAFETY: the unique token retires these live arms before reuse or reentry.
        unsafe {
            ptr::drop_in_place(ptr::addr_of_mut!((*fields).body.output).cast::<F::Output>());
            ptr::drop_in_place(ptr::addr_of_mut!((*fields).data).cast::<M>());
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Token<'static, (), ()>>() == 0);
    assert!(
        mem::size_of::<Value<'static, abi::Ready<()>, (), (), [u64; 2]>>()
            == mem::size_of::<Value<'static, abi::Ready<()>, (), (), ()>>()
    );
};
