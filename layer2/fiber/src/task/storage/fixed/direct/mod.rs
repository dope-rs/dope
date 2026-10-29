use std::{marker, pin, task};

use o3::alloc;

use crate::{
    abi, context,
    task::storage::{
        Id,
        fixed::{
            direct, local,
            quota::{self, State},
            staged, tree,
        },
        scope::{Domain, Scope, Scoped},
    },
};

/// Allocation-free description of a bounded direct task plane.
pub struct Plan<'d, R: tree::Tree<'d>> {
    limit: quota::Limit,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    tree: marker::PhantomData<fn() -> R>,
}

impl<'d, R: tree::Tree<'d>> Plan<'d, R> {
    pub fn new(limit: quota::Limit) -> Self {
        Self {
            limit,
            driver: marker::PhantomData,
            tree: marker::PhantomData,
        }
    }
}

impl<'d, R: tree::Build<'d>> Plan<'d, R> {
    pub fn try_prepare(self) -> Result<Plane<'d, R, quota::Bounded>, alloc::Error> {
        Ok(Plane::bounded(R::try_build()?, self.limit))
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Plane<'d, R, Q = quota::Physical>
where
    R: tree::Tree<'d>,
{
    #[pin]
    routes: R,
    quota: Q,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, R> Plane<'d, R, quota::Physical>
where
    R: tree::Tree<'d>,
{
    pub fn new(routes: R) -> Self {
        Self::with_quota(routes, quota::Physical)
    }
}

impl<'d, R> Plane<'d, R, quota::Bounded>
where
    R: tree::Tree<'d>,
{
    pub fn bounded(routes: R, limit: quota::Limit) -> Self {
        let tasks = R::TASKS;
        Self::with_quota(routes, quota::Bounded::new(limit, tasks))
    }

    /// Returns the remaining global admission quota.
    pub const fn available(&self) -> quota::Remaining {
        self.quota.available()
    }
}

impl<'d, R, Q> Plane<'d, R, Q>
where
    R: tree::Tree<'d>,
    Q: State,
{
    fn with_quota(routes: R, quota: Q) -> Self {
        Self {
            routes,
            quota,
            driver: marker::PhantomData,
        }
    }

    pub(crate) fn route<P>(self: pin::Pin<&mut Self>) -> Route<'_, 'd, R, P, Q>
    where
        R: tree::At<'d, P>,
    {
        Route {
            plane: self,
            path: marker::PhantomData,
        }
    }
}

impl<'d, R> Default for Plane<'d, R, quota::Physical>
where
    R: tree::Tree<'d> + Default,
{
    fn default() -> Self {
        Self::new(R::default())
    }
}

pub struct Route<'a, 'd, R, P, Q = quota::Physical>
where
    R: tree::At<'d, P>,
{
    plane: pin::Pin<&'a mut Plane<'d, R, Q>>,
    path: marker::PhantomData<fn() -> P>,
}

#[must_use = "an admission must be consumed by insertion"]
struct Admission<'a, A, Q> {
    slot: A,
    quota: &'a mut Q,
    vacancy: quota::Vacancy<Q>,
}

pub trait Insert<'d, D> {
    type Fiber;

    fn insert(self, fiber: Self::Fiber) -> Id<'d, D>;
}

/// In-place admission retaining its original storage scope.
pub trait Source<'d, D>: local::Store<'d, D> {
    fn admit(self: pin::Pin<&mut Self>) -> Option<impl Insert<'d, D, Fiber = Self::Fiber>>;
}

mod core;
pub(in crate::task::storage) use crate::task::storage::fixed::direct::core::Core;

impl<'d, D, A, Q> Insert<'d, D> for Admission<'_, A, Q>
where
    A: Insert<'d, D>,
    Q: State,
{
    type Fiber = A::Fiber;

    fn insert(self, fiber: Self::Fiber) -> Id<'d, D> {
        let id = self.slot.insert(fiber);
        *self.quota = State::consume(*self.quota, self.vacancy);
        id
    }
}

impl<'a, 'd, R, P, Q> local::Core<'d, <R::Member as tree::Member<'d>>::Domain>
    for Route<'a, 'd, R, P, Q>
where
    R: tree::At<'d, P>,
    R::Member: Core<
            'd,
            <R::Member as tree::Member<'d>>::Domain,
            Fiber = <R::Member as tree::Member<'d>>::Fiber,
        >,
    Q: State,
    'd: 'a,
{
    type Fiber = <R::Member as tree::Member<'d>>::Fiber;

    fn capacity(&self) -> super::Capacity {
        let plane = self.plane.as_ref().get_ref();
        let member = R::project_ref(&plane.routes);
        if matches!(Q::capacity(plane.quota), super::Capacity::Full)
            || matches!(local::Core::capacity(member), super::Capacity::Full)
        {
            super::Capacity::Full
        } else {
            super::Capacity::Available
        }
    }

    fn insert(
        self: pin::Pin<&mut Self>,
        fiber: Self::Fiber,
    ) -> Option<Id<'d, <R::Member as tree::Member<'d>>::Domain>> {
        Some(Core::admit(self)?.insert(fiber))
    }

    fn try_remove(
        self: pin::Pin<&mut Self>,
        id: Id<'d, <R::Member as tree::Member<'d>>::Domain>,
    ) -> Result<(), Id<'d, <R::Member as tree::Member<'d>>::Domain>> {
        let route = self.get_mut();
        let plane = route.plane.as_mut().project();
        let member = R::project(plane.routes);
        local::Core::try_remove(member, id)?;
        *plane.quota = State::retire(*plane.quota, quota::Retired::new());
        Ok(())
    }
}

impl<'a, 'd, R, P, Q> local::Next<'d, <R::Member as tree::Member<'d>>::Domain>
    for Route<'a, 'd, R, P, Q>
where
    R: tree::At<'d, P>,
    R::Member: local::Next<
            'd,
            <R::Member as tree::Member<'d>>::Domain,
            Fiber = <R::Member as tree::Member<'d>>::Fiber,
        > + Core<'d, <R::Member as tree::Member<'d>>::Domain>,
    Q: State,
    'd: 'a,
{
    fn poll_next(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, <R::Member as tree::Member<'d>>::Domain>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<Option<<Self::Fiber as abi::Stream<'d>>::Item>>> {
        let plane = self.get_mut().plane.as_mut().project();
        local::Next::poll_next(R::project(plane.routes), id, context)
    }
}

impl<'a, 'd, R, P, Q> Core<'d, <R::Member as tree::Member<'d>>::Domain> for Route<'a, 'd, R, P, Q>
where
    R: tree::At<'d, P>,
    R::Member: Core<
            'd,
            <R::Member as tree::Member<'d>>::Domain,
            Fiber = <R::Member as tree::Member<'d>>::Fiber,
        >,
    Q: State,
    'd: 'a,
{
    fn admit(
        self: pin::Pin<&mut Self>,
    ) -> Option<impl Insert<'d, <R::Member as tree::Member<'d>>::Domain, Fiber = Self::Fiber>> {
        let plane = self.get_mut().plane.as_mut().project();
        let vacancy = Q::vacancy(*plane.quota)?;
        let slot = Core::admit(R::project(plane.routes))?;
        Some(Admission {
            slot,
            quota: plane.quota,
            vacancy,
        })
    }
}

impl<'a, 'd, R, P, Q> staged::Core<'d, <R::Member as tree::Member<'d>>::Domain>
    for Route<'a, 'd, R, P, Q>
where
    R: tree::At<'d, P>,
    R::Member: staged::Core<
            'd,
            <R::Member as tree::Member<'d>>::Domain,
            Fiber = <R::Member as tree::Member<'d>>::Fiber,
        > + Core<'d, <R::Member as tree::Member<'d>>::Domain>,
    Q: State,
    'd: 'a,
{
    type Ready<'b>
        = Completed<
        'b,
        <R::Member as staged::Core<'d, <R::Member as tree::Member<'d>>::Domain>>::Ready<'b>,
        Q,
    >
    where
        Self: 'b;

    fn poll_ready<'b>(
        self: pin::Pin<&'b mut Self>,
        id: &Id<'d, <R::Member as tree::Member<'d>>::Domain>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<(Self::Ready<'b>, bool)>> {
        let plane = self.get_mut().plane.as_mut().project();
        let polled = staged::Core::poll_ready(R::project(plane.routes), id, context)?;
        Some(polled.map(|(ready, fresh)| {
            (
                Completed {
                    ready,
                    quota: plane.quota,
                },
                fresh,
            )
        }))
    }

    fn poll_staged(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, <R::Member as tree::Member<'d>>::Domain>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<staged::Poll> {
        let route = self.get_mut();
        let plane = route.plane.as_mut().project();
        let member = R::project(plane.routes);
        staged::Core::poll_staged(member, id, context)
    }

    fn take_output(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, <R::Member as tree::Member<'d>>::Domain>,
    ) -> Option<<Self::Fiber as abi::Fiber<'d>>::Output> {
        let route = self.get_mut();
        let plane = route.plane.as_mut().project();
        let member = R::project(plane.routes);
        staged::Core::take_output(member, id)
    }

    fn state(&self, id: &Id<'d, <R::Member as tree::Member<'d>>::Domain>) -> Option<staged::State> {
        let plane = self.plane.as_ref().get_ref();
        let member = R::project_ref(&plane.routes);
        staged::Core::state(member, id)
    }

    fn remove_state(
        self: pin::Pin<&mut Self>,
        id: Id<'d, <R::Member as tree::Member<'d>>::Domain>,
    ) -> Option<staged::State> {
        let plane = self.get_mut().plane.as_mut().project();
        let state = staged::Core::remove_state(R::project(plane.routes), id)?;
        *plane.quota = State::retire(*plane.quota, quota::Retired::new());
        Some(state)
    }
}

pub struct Completed<'a, C, Q> {
    ready: C,
    quota: &'a mut Q,
}

impl<C: staged::Completion, Q: State> staged::Completion for Completed<'_, C, Q> {
    type Output = C::Output;

    fn finish(self) -> Self::Output {
        let output = self.ready.finish();
        *self.quota = self.quota.retire(quota::Retired::new());
        output
    }
}

struct Bound<'s, A>(Scope<'s, A>);

impl<'s, 'd, D, A: direct::Insert<'d, D>> direct::Insert<'d, Domain<'s, D>> for Bound<'s, A> {
    type Fiber = A::Fiber;

    fn insert(self, fiber: Self::Fiber) -> Scoped<'s, 'd, D> {
        self.0.source.insert(fiber).bind(self.0.proof)
    }
}

impl<'s, 'd, D, S: direct::Core<'d, D> + Unpin> direct::Source<'d, Domain<'s, D>> for Scope<'s, S> {
    fn admit(
        self: pin::Pin<&mut Self>,
    ) -> Option<impl direct::Insert<'d, Domain<'s, D>, Fiber = Self::Fiber>> {
        let this = self.get_mut();
        let permit = pin::Pin::new(&mut this.source).admit()?;
        Some(Bound(Scope::new(permit, this.proof)))
    }
}
