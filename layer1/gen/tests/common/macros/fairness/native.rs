use std::{cell::Cell, marker::PhantomPinned, ops, pin::Pin, rc::Rc};

use engine::{
    core::{
        driver::{route::KeyTag, schedule},
        io,
        o3::mem::quota,
    },
    manifold::app::dispatch::{self, event},
    runtime::executor,
};

use crate::common::macros::fairness::Child;

pub struct Count {
    active: Rc<Cell<bool>>,
    count: Rc<Cell<usize>>,
    _pinned: PhantomPinned,
}

impl Count {
    pub fn new(active: &Rc<Cell<bool>>, count: &Rc<Cell<usize>>) -> Self {
        Self {
            active: Rc::clone(active),
            count: Rc::clone(count),
            _pinned: PhantomPinned,
        }
    }
}

impl<'d> event::Drive<'d> for Count {
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

// SAFETY: Count retains no driver-visible pointer and remains pinned in Mixed
// throughout the installed application lifecycle.
unsafe impl<'d> dispatch::raw::Manifold<'d> for Count {
    type Route = KeyTag<0>;
    type Dispatch = event::Driven<dispatch::raw::Plain>;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Omit;
    type Activate = dispatch::raw::Omit;
    type Probe = dispatch::raw::Omit;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize = 1;

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
            let mut work = turn.coord();
            while let quota::Take::Taken = work.take() {
                this.count.set(this.count.get() + 1);
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
pub struct Mixed {
    #[pin]
    #[manifold]
    pub first: Count,
    #[pin]
    #[app]
    pub child: Child,
}
