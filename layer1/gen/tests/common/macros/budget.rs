use std::{
    cell::Cell,
    marker::{PhantomData, PhantomPinned},
    ops,
    pin::Pin,
    rc::Rc,
};

use engine::{
    core::{
        driver::{route::Identity, schedule},
        io,
    },
    manifold::app::dispatch::{self, event},
    runtime::executor,
};

pub struct Budget<ID: Identity> {
    pub first: Rc<Cell<Option<(usize, usize)>>>,
    pub marker: PhantomData<fn() -> ID>,
    pub _pinned: PhantomPinned,
}

impl<'d, ID: Identity> event::Drive<'d> for Budget<ID> {
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

// SAFETY: Budget retains no driver-visible pointer and remains pinned within
// Metered for the complete run.
unsafe impl<'d, ID: Identity> dispatch::raw::Manifold<'d> for Budget<ID> {
    const COORDS: usize = 0;

    type Route = ID;
    type Dispatch = event::Driven<dispatch::raw::Plain>;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Omit;
    type Activate = dispatch::raw::Omit;
    type Probe = dispatch::raw::Omit;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let this = self.as_ref().get_ref();
        if this.first.get().is_some() {
            return self;
        }
        let mut maint = 0;
        while turn.maint().permit().is_some() {
            maint += 1;
        }
        let mut app = 0;
        while turn.app().permit().is_some() {
            app += 1;
        }
        this.first.set(Some((maint, app)));
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
