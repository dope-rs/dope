use std::{marker, pin};

use o3::alloc;
use schedule::{result::Selection, selected};

use crate::{
    core::driver::schedule::{app, ready::task::owner},
    task::storage::fixed::{
        quota::{self, State},
        schedule::{self, queue, ready::Global},
        tree,
    },
};

#[pin_project::pin_project(!Unpin)]
pub(super) struct Active<R, Q> {
    #[pin]
    pub(super) routes: R,
    pub(super) quota: Q,
}

/// A live queued task paired with the routes that admitted it.
pub(super) struct Ready<'a, R> {
    routes: pin::Pin<&'a mut R>,
    index: Global,
}

impl<'a, R> Ready<'a, R> {
    pub(super) fn into_parts(self) -> (pin::Pin<&'a mut R>, Global) {
        (self.routes, self.index)
    }
}

/// Admission taken from the scheduler paired with this exact route owner.
/// Fields remain private so routing cannot combine different planes.
pub(super) struct Dequeued<'a, 'd, R, Q, S>
where
    S: queue::Backend<'d> + 'a,
{
    active: pin::Pin<&'a mut Active<R, Q>>,
    dequeued: S::Dequeued<'a, Global>,
}

impl<'a, 'd, R, Q, S> Dequeued<'a, 'd, R, Q, S>
where
    S: queue::Backend<'d> + 'a,
{
    pub(super) fn into_parts(self) -> (pin::Pin<&'a mut Active<R, Q>>, S::Dequeued<'a, Global>) {
        (self.active, self.dequeued)
    }
}

impl<R, Q> Active<R, Q> {
    pub(super) fn split(self: pin::Pin<&mut Self>) -> (pin::Pin<&mut R>, &mut Q) {
        let active = self.project();
        (active.routes, active.quota)
    }
}

/// Pinned owner of route storage and every intrusive ready-queue endpoint.
///
#[pin_project::pin_project(!Unpin)]
pub struct Plane<'d, R, Q = quota::Physical, S = queue::Flat<'d>>
where
    R: tree::Tree<'d>,
{
    #[pin]
    pub(super) active: Active<R, Q>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    #[pin]
    scheduler: S,
}

impl<'d, R> Plane<'d, R, quota::Physical, queue::Flat<'d>>
where
    R: tree::Tree<'d>,
{
    pub fn from_routes(routes: R) -> Self {
        Self::from_parts(routes, quota::Physical, queue::Flat::new())
    }
}

impl<'d, R> Plane<'d, R, quota::Physical, queue::Flat<'d>>
where
    R: schedule::PlaneTree<'d>,
{
    pub fn try_new() -> Result<Self, alloc::Error> {
        Ok(Self::from_routes(R::try_build()?))
    }
}

impl<'d, R> Plane<'d, R, quota::Bounded, queue::Flat<'d>>
where
    R: tree::Tree<'d>,
{
    pub fn bounded(routes: R, limit: quota::Limit) -> Self {
        let tasks = R::TASKS;
        Self::from_parts(
            routes,
            quota::Bounded::new(limit, tasks),
            queue::Flat::new(),
        )
    }

    /// Returns the remaining global admission quota.
    pub const fn available(&self) -> quota::Remaining {
        self.active.quota.available()
    }
}

impl<'d, R, Source> Plane<'d, R, quota::Physical, queue::Owned<'d, Source>>
where
    R: tree::Tree<'d>,
{
    pub fn try_owned(routes: R, owners: owner::Capacity) -> Result<Self, queue::PrepareError> {
        let queue = queue::Owned::try_new(R::TASKS, owners)?;
        Ok(Self::from_parts(routes, quota::Physical, queue))
    }
}

impl<'d, R, Q, S> Plane<'d, R, Q, S>
where
    R: tree::Tree<'d>,
    Q: State,
    S: queue::Backend<'d>,
{
    pub(super) fn from_parts(routes: R, quota: Q, queue: S) -> Self {
        Self {
            active: Active { routes, quota },
            driver: marker::PhantomData,
            scheduler: queue,
        }
    }

    pub(super) fn split(self: pin::Pin<&mut Self>) -> (pin::Pin<&mut R>, &mut Q, pin::Pin<&S>) {
        let plane = self.project();
        let (routes, quota) = plane.active.split();
        (routes, quota, plane.scheduler.into_ref())
    }

    pub(crate) fn route<P>(self: pin::Pin<&mut Self>) -> schedule::Route<'_, 'd, R, P, Q, S>
    where
        R: tree::At<'d, P>,
    {
        schedule::Route {
            plane: self,
            path: marker::PhantomData,
        }
    }
}

impl<'d, R, Q, Source> Plane<'d, R, Q, queue::Owned<'d, Source>>
where
    R: tree::Tree<'d>,
    Q: State,
{
    pub fn owners(self: pin::Pin<&Self>) -> queue::Owners<'_, 'd, Source> {
        queue::Owners::new(self.project_ref().scheduler)
    }
}

impl<'d, R, Q> Plane<'d, R, Q, queue::Flat<'d>>
where
    R: schedule::PlaneTree<'d>,
    Q: State,
{
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, R, Q> {
        let plane = self.project();
        let mut active = plane.active;
        let queue = plane.scheduler.into_ref();
        let mut ready = match queue.queue().snapshot() {
            Some(ready) => ready,
            None => return Selection::Idle,
        };
        match work.next(&mut ready) {
            app::Next::Item(dequeued, permit) => {
                ready.pause();
                if let Some(index) = queue.queue().slot().pop()
                    && index != dequeued.index
                {
                    let (routes, _) = active.as_mut().split();
                    R::merge_ready(selected::Access::from_ready(Ready { routes, index }), queue);
                }
                Selection::Ready(selected::Item::new(
                    Dequeued { active, dequeued },
                    work,
                    permit,
                ))
            }
            app::Next::Empty => Selection::Idle,
            app::Next::Exhausted(global) => {
                ready.pause();
                let (routes, _) = active.split();
                R::wake_global(selected::Access::from_ready(Ready {
                    routes,
                    index: global,
                }));
                Selection::Yielded
            }
        }
    }
}

impl<'d, R, Q, Source> Plane<'d, R, Q, queue::Owned<'d, Source>>
where
    R: schedule::PlaneTree<'d>,
    Q: State,
    Source: Copy,
{
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        scope: owner::Scope<Source>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, R, Q, queue::Owned<'d, Source>> {
        let plane = self.project();
        let mut active = plane.active;
        let queue = plane.scheduler.into_ref().queue();
        loop {
            let ((dequeued, moved), permit) = match queue.next(scope, work) {
                app::Next::Item(item, permit) => (item, permit),
                app::Next::Empty => return Selection::Idle,
                app::Next::Exhausted(index) => {
                    let (routes, _) = active.as_mut().split();
                    R::wake_global(selected::Access::from_ready(Ready { routes, index }));
                    return Selection::Yielded;
                }
            };
            if let Some(index) = moved {
                let (routes, _) = active.as_mut().split();
                R::wake_global(selected::Access::from_ready(Ready { routes, index }));
            }
            if let Some(dequeued) = dequeued {
                return Selection::Ready(selected::Item::new(
                    Dequeued { active, dequeued },
                    work,
                    permit,
                ));
            }
            permit.run(|| ());
        }
    }

    pub fn wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) {
        let _ = self.try_wake(scope);
    }

    pub fn try_wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) -> queue::Wake {
        let plane = self.project();
        let (task, wake) = match plane.scheduler.into_ref().queue().front(scope) {
            Some(front) => front,
            None => return queue::Wake::Idle,
        };
        let (routes, _) = plane.active.split();
        R::wake_global(selected::Access::from_ready(Ready {
            routes,
            index: task,
        }));
        wake
    }
}
