use std::{
    cell::Cell,
    marker::{PhantomData, PhantomPinned},
    ops,
    pin::{self, Pin},
    rc::Rc,
};

use engine::{
    core::{
        driver::{route::Identity, schedule, schedule::Progress, settings},
        io,
    },
    manifold::app::dispatch::{self, event},
    runtime::executor,
};

#[derive(Default)]
pub struct Counts {
    pub activate: Rc<Cell<u32>>,
    pub tick: Rc<Cell<u32>>,
    pub install: Rc<Cell<u32>>,
    pub shutdown: Rc<Cell<u32>>,
    pub finish: Rc<Cell<u32>>,
}

pub struct Counter<ID: Identity, M = ()> {
    pub calls: Counts,
    pub marker: PhantomData<fn() -> (M, ID)>,
    pub _pinned: PhantomPinned,
}

impl<ID: Identity, M> Default for Counter<ID, M> {
    fn default() -> Self {
        Self {
            calls: Counts::default(),
            marker: PhantomData,
            _pinned: PhantomPinned,
        }
    }
}

impl<'d, ID: Identity, M> dispatch::raw::Install<'d> for Counter<ID, M> {
    fn install(self: pin::Pin<&mut Self>) {
        let calls = &self.as_ref().get_ref().calls.install;
        calls.set(calls.get() + 1);
    }
}

impl<'d, ID: Identity, M> executor::raw::Finishes<'d, Self> for Counter<ID, M> {
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (owner, install, proof) = finish.into_parts();
        let calls = &owner.as_ref().get_ref().calls.finish;
        calls.set(calls.get() + 1);
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<'d, ID: Identity, M> event::Drive<'d> for Counter<ID, M> {
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

// SAFETY: Counter retains no driver-visible pointer and is pinned by every
// generated dispatcher that owns it.
unsafe impl<'d, ID: Identity, M> dispatch::raw::Manifold<'d> for Counter<ID, M> {
    const COORDS: usize = 0;

    type Route = ID;
    type Dispatch = event::Driven<dispatch::raw::Plain>;
    type Install = dispatch::raw::Plain;
    type Finish = Self;
    type Activate = Self;
    type Probe = Self;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let this = self.as_ref().get_ref();
        this.calls.tick.set(this.calls.tick.get() + 1);
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
        let calls = &self.as_ref().get_ref().calls.shutdown;
        calls.set(calls.get() + 1);
        self
    }
}

// SAFETY: unit selection binds every query to this exact pinned counter.
unsafe impl<'d, ID: Identity, M> dispatch::raw::Probe<'d, Self> for Counter<ID, M> {
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(_: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> Progress<'d> {
        Progress::Quiescent
    }
}

// SAFETY: selection retains one owned plan for this exact counter route.
unsafe impl<'d, ID: Identity, M> dispatch::raw::Wakes<'d, Self, ID> for Counter<ID, M> {
    type Plan = Cell<()>;

    const ROUTES: settings::Routes = settings::Routes::singleton::<ID>();

    fn select() -> Option<Self::Plan> {
        Some(Cell::new(()))
    }

    unsafe fn call<'a, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'a,
            Self,
            Self::Plan,
            executor::raw::Activate<'driver, 'turn, 'root, 'd>,
        >,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        let (owner, plan, _) = call.into_parts();
        let () = plan.into_inner();
        let this = owner.as_ref().get_ref();
        this.calls.activate.set(this.calls.activate.get() + 1);
    }
}
