use std::{marker, pin, task};

use o3::{
    alloc,
    depot::arena::slab::{Anchored, storage},
};

use crate::{
    abi, context,
    task::storage::{
        Id, Owner, caps,
        fixed::{self, direct, local, staged, store, tree},
        scope::{Domain, Scope, Scoped},
        slots,
    },
};

mod poll;
mod vacant;

pub use poll::Poll;
pub use slots::State;

/// A retained completion whose output and physical slot are consumed together.
/// Dropping the capability leaves the staged output in its original slot.
pub trait Completion {
    type Output;

    fn finish(self) -> Self::Output;
}

pub struct Ready<'a, F, O, Tag = (), const N: usize = 1, S: storage::Storage = storage::Inline<N>> {
    member: Anchored<'a, slots::raw::Entry<F, O>, Tag, { u32::MAX }, false, 1, S>,
}

impl<F, O, Tag, const N: usize, S: storage::Storage> Completion for Ready<'_, F, O, Tag, N, S> {
    type Output = O;

    fn finish(mut self) -> O {
        // SAFETY: poll_ready established Done while retaining this exact
        // member's exclusive pin; no operation can replace or poll it meanwhile.
        let output = unsafe { slots::raw::Done::exact(self.member.value()) }.take();
        self.member.discard();
        output
    }
}

/// Fixed slots that reuse completed fibers as in-place outputs.
/// The live/staged/vacant phase machine is a fiber lifecycle, so only the slots
/// themselves and their pinned access are delegated to o3.
#[pin_project::pin_project(!Unpin)]
pub struct Slab<'d, F, const N: usize, D, S: storage::Storage = storage::Inline<N>>
where
    F: abi::Fiber<'d>,
{
    #[pin]
    inner: store::Store<slots::raw::Entry<F, F::Output>, N, D, S>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F, const N: usize, D> Slab<'d, F, N, D>
where
    F: abi::Fiber<'d>,
{
    pub fn new() -> Self {
        Self {
            inner: store::Store::new(caps::Tasks::new()),
            driver: marker::PhantomData,
        }
    }
}

impl<'d, F, const N: usize, D> Default for Slab<'d, F, N, D>
where
    F: abi::Fiber<'d>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, F: abi::Fiber<'d>, const N: usize, D> Slab<'d, F, N, D, storage::Heap> {
    /// Allocates the exact slot extent before constructing its entries.
    pub fn try_new() -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: store::Store::try_new(caps::Tasks::new())?,
            driver: marker::PhantomData,
        })
    }
}

/// In-place completion access retaining its original storage scope.
pub trait Store<'d, D>: local::Store<'d, D, Fiber: abi::Fiber<'d>> {
    type Ready<'a>: Completion<Output = <Self::Fiber as abi::Fiber<'d>>::Output>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>>;

    fn poll_staged(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<Poll>;
    fn take_output(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
    ) -> Option<<Self::Fiber as abi::Fiber<'d>>::Output>;
    fn state(&self, id: &Id<'d, D>) -> Option<State>;

    /// Removes one identity, returning its phase before destruction.
    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Option<State>;
}

mod core;
pub(in crate::task::storage) use crate::task::storage::fixed::staged::core::Core;

impl<'d, F, const N: usize, D, S: storage::Storage> local::Core<'d, D> for Slab<'d, F, N, D, S>
where
    F: abi::Fiber<'d>,
{
    type Fiber = F;

    fn capacity(&self) -> fixed::Capacity {
        self.inner.capacity()
    }

    fn insert(self: pin::Pin<&mut Self>, fiber: F) -> Option<Id<'d, D>> {
        let vacant = direct::Core::admit(self)?;
        Some(direct::Insert::insert(vacant, fiber))
    }

    fn try_remove(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Result<(), Id<'d, D>> {
        self.project().inner.try_remove(id)
    }
}

impl<'d, F: abi::Fiber<'d>, const N: usize, D, S: storage::Storage> direct::Core<'d, D>
    for Slab<'d, F, N, D, S>
{
    fn admit(self: pin::Pin<&mut Self>) -> Option<impl direct::Insert<'d, D, Fiber = F>> {
        let this = self.project();
        Some(vacant::Vacant {
            slot: this.inner.vacant()?,
            driver: *this.driver,
        })
    }
}

impl<'d, F, const N: usize, D, S: storage::Storage> staged::Core<'d, D> for Slab<'d, F, N, D, S>
where
    F: abi::Fiber<'d>,
{
    type Ready<'a>
        = Ready<'a, F, F::Output, D, N, S>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Id<'d, D>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>> {
        let key = id.key_for(Owner::of(&self.inner))?;
        let mut member = self.project().inner.member(key.parts())?;
        let fresh = match member.value().poll_fiber(context.as_mut())? {
            task::Poll::Pending => return Some(task::Poll::Pending),
            task::Poll::Ready((_, fresh)) => fresh,
        };
        Some(task::Poll::Ready((Ready { member }, fresh)))
    }

    fn poll_staged(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<Poll> {
        let key = id.key_for(Owner::of(&self.inner))?;
        let slot = self.project().inner.parts_mut(key.parts())?;
        Some(match slot.poll_fiber(context.as_mut())? {
            task::Poll::Pending => Poll::Pending,
            task::Poll::Ready((_, true)) => Poll::NewlyReady,
            task::Poll::Ready((_, false)) => Poll::Staged,
        })
    }

    fn take_output(self: pin::Pin<&mut Self>, id: &Id<'d, D>) -> Option<F::Output> {
        let key = id.key_for(Owner::of(&self.inner))?;
        let slot = self.project().inner.parts_mut(key.parts())?;
        slot.done().map(slots::raw::Done::take)
    }

    fn state(&self, id: &Id<'d, D>) -> Option<State> {
        let key = id.key_for(Owner::of(&self.inner))?;
        self.inner.parts(key.parts()).map(slots::raw::Entry::state)
    }

    fn remove_state(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Option<State> {
        let key = id.key_for(Owner::of(&self.inner))?;
        let member = self.project().inner.member(key.parts())?;
        let state = member.get().state();
        member.discard();
        Some(state)
    }
}

impl<'d, F, const N: usize, D, S: storage::Storage> tree::Member<'d> for Slab<'d, F, N, D, S>
where
    F: abi::Fiber<'d>,
{
    type Fiber = F;
    type Domain = D;
    type Target = ();

    const CAPACITY: tree::Capacity = tree::Capacity::new(caps::Tasks::<N>::new().nonzero());
}

impl<'d, F: abi::Fiber<'d>, const N: usize, D> tree::Build<'d>
    for tree::Leaf<Slab<'d, F, N, D, storage::Heap>>
{
    fn try_build() -> Result<Self, alloc::Error> {
        Ok(Self::new(Slab::try_new()?))
    }
}

impl<'s, 'd, D, S: staged::Core<'d, D> + Unpin> staged::Store<'d, Domain<'s, D>> for Scope<'s, S> {
    type Ready<'a>
        = S::Ready<'a>
    where
        Self: 'a;

    fn poll_ready<'a>(
        self: pin::Pin<&'a mut Self>,
        id: &Scoped<'s, 'd, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'a>, bool)>> {
        pin::Pin::new(&mut self.get_mut().source).poll_ready(&id.original(), context)
    }

    fn poll_staged(
        self: pin::Pin<&mut Self>,
        id: &Scoped<'s, 'd, D>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<staged::Poll> {
        pin::Pin::new(&mut self.get_mut().source).poll_staged(&id.original(), context)
    }

    fn take_output(
        self: pin::Pin<&mut Self>,
        id: &Scoped<'s, 'd, D>,
    ) -> Option<<Self::Fiber as abi::Fiber<'d>>::Output> {
        pin::Pin::new(&mut self.get_mut().source).take_output(&id.original())
    }

    fn state(&self, id: &Scoped<'s, 'd, D>) -> Option<staged::State> {
        self.source.state(&id.original())
    }

    fn remove_state(self: pin::Pin<&mut Self>, id: Scoped<'s, 'd, D>) -> Option<State> {
        pin::Pin::new(&mut self.get_mut().source).remove_state(id.original())
    }
}
