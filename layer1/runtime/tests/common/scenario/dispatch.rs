//! Statically dispatched runtime probes for integration tests.

use std::{convert::Infallible, marker, ops, pin};

use dope_core::{
    driver::{
        retained, route,
        route::Identity,
        schedule,
        schedule::{clock, ready},
        settings,
    },
    io,
};
use dope_runtime::executor;
use o3::base::cell::region;
use route::kind;

/// Whether the exact event supplied to a probe is consumed or deferred.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Consume,
    Defer,
}

/// Direct lifecycle behavior and the retained park mode of one runtime probe.
pub trait Hooks<'d, R>: Unpin {
    type Park;

    const WAKE: bool = false;

    fn dispatch(&mut self, _: &mut R, _: &io::Event<'d>) -> Decision {
        Decision::Consume
    }

    fn activate(
        &mut self,
        _: &mut R,
        _: route::Erased<'d, kind::Any>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
    }

    fn progress(
        &self,
        _: &R,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn shutdown(&mut self, _: &mut R) {}

    fn finish(&mut self, _: &mut R) {}
}

pub trait Park<'d, R>: Hooks<'d, R> {
    fn pre_park(&mut self, ready: &mut R, driver: &mut retained::Context<'_, '_, 'd>);
}

pub struct On;
pub struct Off;

pub struct Builder<H> {
    hooks: H,
}

impl<H> Builder<H> {
    pub const fn new(hooks: H) -> Self {
        Self { hooks }
    }

    pub fn probe<'d, ID: Identity>(self) -> Probe<'d, (), H, ID>
    where
        H: Hooks<'d, ()>,
    {
        Probe::<'_, (), H, ID> {
            ready: (),
            hooks: self.hooks,
            driver: marker::PhantomData,
        }
    }

    pub fn ready<'d, ID: Identity>(
        self,
        ready: ready::Slot<'d, ID>,
    ) -> Probe<'d, ready::Slot<'d, ID>, H, ID>
    where
        H: Hooks<'d, ready::Slot<'d, ID>>,
    {
        Probe::<'_, ready::Slot<'d, ID>, H, ID> {
            ready,
            hooks: self.hooks,
            driver: marker::PhantomData,
        }
    }

    pub fn ready_set<'d, ID: Identity>(
        self,
        ready: Box<[ready::Slot<'d, ID>]>,
    ) -> Probe<'d, Box<[ready::Slot<'d, ID>]>, H, ID>
    where
        H: Hooks<'d, Box<[ready::Slot<'d, ID>]>>,
    {
        Probe::<'_, Box<[ready::Slot<'d, ID>]>, H, ID> {
            ready,
            hooks: self.hooks,
            driver: marker::PhantomData,
        }
    }
}

/// One statically routed dispatcher probe with no dynamic dispatch or allocation.
pub struct Probe<'d, R, H, ID> {
    ready: R,
    hooks: H,
    driver: marker::PhantomData<fn(&'d (), ID) -> &'d ()>,
}

trait ParkCall<'d, R> {
    type Plan;

    fn select() -> Option<Self::Plan>;

    /// # Safety
    /// The root, prepared plan, and park authority belong to the same installation.
    unsafe fn call<'app, 'driver, 'turn, 'root>(
        app: pin::Pin<&'app mut R>,
        plan: &mut Self::Plan,
        park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root;
}

impl<'d, R> ParkCall<'d, R> for Off {
    type Plan = Infallible;

    fn select() -> Option<Self::Plan> {
        None
    }

    unsafe fn call<'app, 'driver, 'turn, 'root>(
        _: pin::Pin<&'app mut R>,
        plan: &mut Self::Plan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        match *plan {}
    }
}

impl<'d, R, H, ID: Identity> ParkCall<'d, Probe<'d, R, H, ID>> for On
where
    R: Ready<'d>,
    H: Park<'d, R>,
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    /// # Safety
    /// The pinned probe, prepared unit plan, and park authority must belong to
    /// the same installed root; its hooks retain no authority beyond this call.
    unsafe fn call<'app, 'driver, 'turn, 'root>(
        mut app: pin::Pin<&'app mut Probe<'d, R, H, ID>>,
        (): &mut Self::Plan,
        park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        let mut driver = park.into_driver();
        let this = app.as_mut().get_mut();
        this.hooks.pre_park(&mut this.ready, &mut driver);
    }
}

trait Ready<'d>: Unpin {}

impl<'d> Ready<'d> for () {}
impl<'d, Tag: route::Tag> Ready<'d> for ready::Slot<'d, Tag> {}
impl<'d, Tag: route::Tag> Ready<'d> for Box<[ready::Slot<'d, Tag>]> {}

impl<'d, R, H, L, ID: Identity> executor::raw::Layout<'d> for Probe<'d, R, H, ID>
where
    H: Hooks<'d, R>,
    H::Park: ParkCall<'d, Self, Plan = L>,
{
    type ParkPlan = L;
}

// SAFETY: the probe retains its exact ready owners and direct lifecycle calls.
unsafe impl<'d, R, H, L, ID: Identity> executor::raw::App<'d> for Probe<'d, R, H, ID>
where
    R: Ready<'d>,
    H: Hooks<'d, R>,
    H::Park: ParkCall<'d, Self, Plan = L>,
{
    const ROUTES: settings::Routes = settings::Routes::singleton::<ID>();
    const WAKE_ROUTES: settings::Routes = if H::WAKE {
        Self::ROUTES
    } else {
        settings::Routes::EMPTY
    };
    const COORDS: usize = 0;

    fn select_park() -> Option<Self::ParkPlan> {
        H::Park::select()
    }

    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: pin::Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: the exact installed probe, selected plan, and authority are unchanged.
        unsafe { H::Park::call(app, plan, park) }
    }

    // SAFETY: the route check retains this exact installed pinned probe.
    unsafe fn activate<'app, 'driver, 'turn, 'root>(
        mut app: pin::Pin<&'app mut Self>,
        wake: executor::raw::Activate<'driver, 'turn, 'root, 'd>,
    ) -> bool
    where
        'd: 'driver + 'turn + 'root,
    {
        if !Self::WAKE_ROUTES.has_route(wake.target().route()) {
            return false;
        }
        let (target, mut driver) = wake.into_driver();
        let this = app.as_mut().get_mut();
        this.hooks.activate(&mut this.ready, target, &mut driver);
        true
    }

    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Dispatch<'app, 'driver, 'turn, 'root, 'd, Self>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        let (app, event) = call.into_parts();
        if route::Id::try_from(event.route()) != Ok(ID::ID) {
            return ops::ControlFlow::Continue(());
        }
        let this = app.get_mut();
        match this.hooks.dispatch(&mut this.ready, &event) {
            Decision::Consume => ops::ControlFlow::Continue(()),
            Decision::Defer => ops::ControlFlow::Break(event),
        }
    }

    fn progress(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        let this = app.get_ref();
        this.hooks
            .progress(&this.ready, &view.regions().0, view.clock())
    }

    fn stopping(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        Self::progress(app, view)
    }

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        shutdown: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        let (mut app, turn, driver) = shutdown.into_parts();
        let this = app.as_mut().get_mut();
        this.hooks.shutdown(&mut this.ready);
        executor::raw::Shutdown::new(app, turn, driver)
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut app, install, finish) = finish.into_parts();
        let this = app.as_mut().get_mut();
        this.hooks.finish(&mut this.ready);
        executor::raw::Finish::new(app, install, finish)
    }
}
