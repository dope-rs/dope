use std::{cell::Cell, convert::Infallible, pin::Pin, rc::Rc};

use engine::{
    core::driver::{schedule, settings},
    runtime::{control, executor},
};

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
pub struct Idle {
    #[dispatcher(state)]
    pub active: Rc<Cell<bool>>,
    #[dispatcher(state)]
    pub count: Rc<Cell<usize>>,
}

impl Idle {
    fn coordinate(this: idle_coord::Fields<'_, '_>) -> control::Flow {
        idle(this.active, this.count)
    }
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Branch<A, B> {
    #[pin]
    #[app]
    pub first: A,
    #[pin]
    #[app]
    pub second: B,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
pub struct Head<T> {
    #[pin]
    #[app]
    pub tree: T,
    #[dispatcher(state)]
    pub active: Rc<Cell<bool>>,
    #[dispatcher(state)]
    pub count: Rc<Cell<usize>>,
}

impl<T> Head<T> {
    fn coordinate(this: head_coord::Fields<'_, '_, T>) -> control::Flow {
        idle(this.active, this.count)
    }
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Absent<T> {
    #[pin]
    #[app]
    pub first: T,
    #[pin]
    #[app(optional)]
    pub second: Option<T>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Owned<T, G> {
    #[pin]
    #[app]
    pub app: T,
    #[dispatcher(state)]
    pub guard: G,
}

pub struct Unselected;

impl<'d> executor::raw::Layout<'d> for Unselected {
    type ParkPlan = Infallible;
}

// SAFETY: Unselected has no installed owner, route, or driver-visible pointer.
unsafe impl<'d> executor::raw::App<'d> for Unselected {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    const COORDS: usize = 1;

    fn select_park() -> Option<Self::ParkPlan> {
        None
    }

    /// # Safety
    /// The owner and plan must be installed and admitted together. Selection
    /// never produces this uninhabited plan, so no invocation can be admitted.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        _: Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        match *plan {}
    }
}

type Level<T> = Branch<T, T>;
type Four<T> = Level<Level<T>>;
#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Tree {
    #[pin]
    #[app]
    root: Level<Four<Four<Four<Idle>>>>,
}

pub fn new(active: &Rc<Cell<bool>>, counts: &[Rc<Cell<usize>>; 128]) -> Tree {
    let mut next = 0;
    let mut leaf = || {
        let value = Idle {
            active: Rc::clone(active),
            count: Rc::clone(&counts[next]),
        };
        next += 1;
        value
    };
    Tree {
        root: pair(|| four(|| four(|| four(&mut leaf)))),
    }
}

fn pair<T>(mut make: impl FnMut() -> T) -> Level<T> {
    Branch {
        first: make(),
        second: make(),
    }
}

fn four<T>(mut make: impl FnMut() -> T) -> Four<T> {
    pair(|| pair(&mut make))
}

fn idle(active: &Cell<bool>, count: &Cell<usize>) -> control::Flow {
    if active.get() {
        count.set(count.get() + 1);
    }
    control::Flow::Idle
}
