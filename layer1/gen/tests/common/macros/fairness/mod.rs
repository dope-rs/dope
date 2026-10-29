use std::{
    array,
    cell::Cell,
    marker::{PhantomData, PhantomPinned},
    ops,
    pin::Pin,
    rc::Rc,
    task::Poll,
};

use engine::{
    core::{
        driver::{
            route::{Identity, KeyTag},
            schedule,
            schedule::ready,
            settings,
        },
        io,
    },
    manifold::app::dispatch::{self, event},
    runtime::{
        control,
        executor::{self, root},
    },
};

pub mod native;
pub mod tree;

pub const SPARSE_WINDOWS: [usize; 16] = [1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0];
pub const COORD_WINDOWS: [usize; 4] = [schedule::LIMIT; 4];
pub const COORD_TARGET: usize = schedule::LIMIT + 1;
pub const IDLE_WINDOW: [usize; 1] = [schedule::LIMIT];

pub struct Rounds<'a, const N: usize> {
    pub windows: &'a [usize],
    pub active: &'a Cell<bool>,
    pub counts: &'a [Rc<Cell<usize>>; N],
}

#[derive(Debug)]
pub struct Exhausted;

impl<'d, const N: usize> root::Root<'d> for Rounds<'_, N> {
    type Output = Result<[usize; N], Exhausted>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, wake, work, _) = context.into_parts();
        let this = root.get_mut();
        let Some((&left, windows)) = this.windows.split_first() else {
            this.active.set(false);
            return Poll::Ready(Ok(array::from_fn(|index| this.counts[index].get())));
        };
        this.windows = windows;
        this.active.set(true);
        while work.remaining() > left {
            let Some(permit) = work.permit() else {
                this.active.set(false);
                return Poll::Ready(Err(Exhausted));
            };
            permit.run(|| ());
        }
        wake.wake();
        Poll::Pending
    }
}

pub struct Eager<ID: Identity> {
    active: Rc<Cell<bool>>,
    count: Rc<Cell<usize>>,
    marker: PhantomData<fn() -> ID>,
    _pinned: PhantomPinned,
}

impl<ID: Identity> Eager<ID> {
    pub fn new(active: &Rc<Cell<bool>>, count: &Rc<Cell<usize>>) -> Self {
        Self {
            active: Rc::clone(active),
            count: Rc::clone(count),
            marker: PhantomData,
            _pinned: PhantomPinned,
        }
    }
}

impl<'d, ID: Identity> event::Drive<'d> for Eager<ID> {
    type Access = dispatch::raw::Plain;

    fn dispatch<'app, 'driver, 'root>(
        _: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        ops::ControlFlow::Continue(())
    }
}

// SAFETY: Eager retains no driver-visible pointer and remains pinned in the
// installed application for the complete run.
unsafe impl<'d, ID: Identity> dispatch::raw::Manifold<'d> for Eager<ID> {
    type Route = ID;
    type Dispatch = event::Driven<dispatch::raw::Plain>;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Omit;
    type Activate = dispatch::raw::Omit;
    type Probe = dispatch::raw::Omit;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let this = self.as_ref().get_ref();
        if this.active.get() {
            while let Some(permit) = turn.app().permit() {
                permit.run(|| this.count.set(this.count.get() + 1));
            }
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        self: Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self
    }
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Pair<A: Identity, B: Identity> {
    #[pin]
    #[manifold]
    pub first: Eager<A>,
    #[pin]
    #[manifold]
    pub second: Eager<B>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Nested {
    #[pin]
    #[app]
    pub first: Pair<KeyTag<0>, KeyTag<1>>,
    #[pin]
    #[app]
    pub second: Pair<KeyTag<2>, KeyTag<3>>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
pub struct Child {
    #[dispatcher(state)]
    pub active: Rc<Cell<bool>>,
    #[dispatcher(state)]
    pub count: Rc<Cell<usize>>,
}

impl Child {
    fn coordinate(this: child_coord::Fields<'_, '_>) -> control::Flow {
        if !this.active.get() {
            return control::Flow::Idle;
        }
        this.count.set(this.count.get() + 1);
        control::Flow::More
    }
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
pub struct Parent {
    #[pin]
    #[app]
    pub child: Child,
    #[dispatcher(state)]
    pub active: Rc<Cell<bool>>,
    #[dispatcher(state)]
    pub count: Rc<Cell<usize>>,
}

impl Parent {
    fn coordinate(this: parent_coord::Fields<'_, '_>) -> control::Flow {
        if !this.active.get() {
            return control::Flow::Idle;
        }
        this.count.set(this.count.get() + 1);
        control::Flow::More
    }
}

#[derive(Default)]
pub struct Deferred<'d> {
    pub calls: Cell<usize>,
    wake: Cell<Option<ready::Target<'d>>>,
}

pub struct AwaitWork<'d> {
    pub observed: Rc<Deferred<'d>>,
}

impl<'d> root::Root<'d> for AwaitWork<'d> {
    type Output = usize;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, wake, _, _) = context.into_parts();
        let observed = &root.get_mut().observed;
        if observed.calls.get() == COORD_TARGET {
            observed.wake.set(None);
            return Poll::Ready(observed.calls.get());
        }
        observed.wake.set(Some(wake));
        Poll::Pending
    }
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine, driver = 'd)]
#[coordinate]
pub struct Limited<'d> {
    #[pin]
    #[app]
    pub child: Child,
    #[dispatcher(state)]
    pub observed: Rc<Deferred<'d>>,
}

impl<'d> Limited<'d> {
    fn coordinate(this: limited_coord::Fields<'_, 'd>) -> control::Flow {
        let Some(wake) = this.observed.wake.get() else {
            return control::Flow::Idle;
        };
        if this.observed.calls.get() == COORD_TARGET {
            return control::Flow::Idle;
        }
        let calls = this.observed.calls.get() + 1;
        this.observed.calls.set(calls);
        if calls == COORD_TARGET {
            wake.wake();
            control::Flow::Idle
        } else {
            control::Flow::More
        }
    }
}

#[derive(Default)]
pub struct PlanObserver {
    pub prepared: Cell<usize>,
    pub parks: Cell<usize>,
    pub dropped: Cell<Option<usize>>,
    pub finished_at_drop: Cell<bool>,
    pub repeated: Cell<bool>,
    finished: Cell<bool>,
}

pub struct Plan {
    polls: usize,
    observed: Option<Rc<PlanObserver>>,
}

impl Drop for Plan {
    fn drop(&mut self) {
        if let Some(observed) = &self.observed {
            observed.finished_at_drop.set(observed.finished.get());
            let previous = observed.dropped.replace(Some(self.polls));
            observed
                .repeated
                .set(observed.repeated.get() || previous.is_some());
        }
    }
}

pub struct PlanOwner {
    pub observed: Rc<PlanObserver>,
}

impl<'d> executor::raw::Layout<'d> for PlanOwner {
    type ParkPlan = Plan;
}

// SAFETY: PlanOwner contains no installed owner or driver-visible pointer.
unsafe impl<'d> executor::raw::App<'d> for PlanOwner {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    const COORDS: usize = 0;

    fn select_park() -> Option<Self::ParkPlan> {
        Some(Plan {
            polls: 0,
            observed: None,
        })
    }

    fn prepare_park(app: Pin<&Self>, _: &mut Self::ParkPlan) -> Option<executor::raw::Count> {
        let observed = &app.get_ref().observed;
        observed.prepared.set(observed.prepared.get() + 1);
        Some(executor::raw::Count::new(Self::COORDS))
    }

    /// # Safety
    /// The installed pinned owner must match this prepared plan and park
    /// authority; the plan retains observations for that same owner.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        let this = app.as_ref().get_ref();
        let observed = plan
            .observed
            .get_or_insert_with(|| Rc::clone(&this.observed));
        plan.polls += 1;
        observed.parks.set(observed.parks.get() + 1);
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (app, install, proof) = finish.into_parts();
        app.as_ref().get_ref().observed.finished.set(true);
        executor::raw::Finish::new(app, install, proof)
    }
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Planned {
    #[pin]
    #[app]
    pub owner: PlanOwner,
}
