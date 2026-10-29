use std::{marker, mem, ops, pin, ptr};

use dope_core::{
    driver::{
        self,
        life::{self, finish},
        retained, route, schedule,
        schedule::clock,
    },
    io,
};
use o3::base::cell::{brand, region};
use route::kind;

use crate::{
    executor::{self, raw::Drain, session},
    topology::lending,
};

/// Installed application accepting ordinary work.
pub(crate) enum Live {}

/// Shutdown has visited every lifecycle field; only draining is legal.
pub(crate) enum Draining {}

#[pin_project::pin_project(project_ref = StoredRef)]
#[repr(C)]
pub(crate) struct Stored<D, Plan> {
    #[pin]
    app: D,
    park: Option<Plan>,
}

pub(crate) struct Backing<'d, D: executor::raw::Layout<'d>> {
    value: mem::ManuallyDrop<brand::Value<brand::Identity<'d>, Stored<D, D::ParkPlan>>>,
}

impl<'d, D: executor::raw::App<'d>> Backing<'d, D> {
    pub(crate) fn new(token: &brand::Token<brand::Identity<'d>>, value: D) -> Self {
        Self::prepared(token, value, None)
    }

    fn prepared(
        token: &brand::Token<brand::Identity<'d>>,
        app: D,
        park: Option<D::ParkPlan>,
    ) -> Self {
        let stored = Stored { app, park };
        Self {
            value: mem::ManuallyDrop::new(token.bind(stored)),
        }
    }

    pub(crate) fn pin(
        self: pin::Pin<&Self>,
    ) -> pin::Pin<&brand::Value<brand::Identity<'d>, Stored<D, D::ParkPlan>>> {
        // SAFETY: the source Pin prevents Backing from moving, and ManuallyDrop
        // keeps the exact Value allocation live until Owner::finish.
        unsafe { self.map_unchecked(|backing| &*backing.value) }
    }
}

struct Key<'app, 'd: 'app, D> {
    anchor: life::raw::Anchor<'app>,
    driver: marker::PhantomData<*mut &'d ()>,
    owner: marker::PhantomData<*mut D>,
}

unsafe impl<'app, 'd: 'app, D> life::raw::Root<'app> for Key<'app, 'd, D> {
    fn anchor(self) -> life::raw::Anchor<'app> {
        self.anchor
    }
}

/// Affine ownership of one exact pinned application lifecycle phase.
pub(crate) struct Owner<'app, 'd: 'app, D: executor::raw::Layout<'d>, S> {
    stored: pin::Pin<&'app brand::Value<brand::Identity<'d>, Stored<D, D::ParkPlan>>>,
    retained: retained::raw::Owner<'app, 'd>,
    install: life::Install<'app, 'd>,
    state: marker::PhantomData<fn(S) -> S>,
}

impl<'app, 'd: 'app, D: executor::raw::App<'d>, S> Owner<'app, 'd, D, S> {
    pub(crate) fn view(&self) -> View<'_, 'd, D, S, D::ParkPlan> {
        View::new(self)
    }
}

impl<'app, 'd: 'app, D> Owner<'app, 'd, D, Live>
where
    D: executor::raw::App<'d>,
{
    pub(crate) fn install(
        stored: pin::Pin<&'app brand::Value<brand::Identity<'d>, Stored<D, D::ParkPlan>>>,
        token: &mut brand::Token<brand::Identity<'d>>,
        driver: driver::Reference<'d>,
    ) -> Self {
        *token.pin_mut(stored).project().park = D::select_park();
        let key: Key<'app, 'd, D> = Key {
            anchor: life::raw::Anchor::new(),
            driver: marker::PhantomData,
            owner: marker::PhantomData,
        };
        let install = life::Install::new(key, driver);
        let install = executor::raw::Install::new(token.pin_mut(stored).project().app, install);
        let install = D::install(install);
        let retained = retained::raw::Owner::from_driver(driver);
        Self {
            stored,
            retained,
            install: install.into_install(),
            state: marker::PhantomData,
        }
    }

    pub(crate) fn admit(
        &self,
        token: &mut brand::Token<brand::Identity<'d>>,
    ) -> Result<(), executor::InstallError> {
        let stored = token.pin_mut(self.stored).project();
        D::admit(
            stored.app.as_ref(),
            &mut driver::settings::claim::Needs::new(),
        )?;
        let Some(plan) = stored.park else {
            return Ok(());
        };
        let count = D::prepare_park(stored.app.as_ref(), plan)
            .ok_or(executor::InstallError::CoordinatorCountOverflow)?;
        count.admit()
    }

    pub(crate) fn shutdown<'turn>(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'_, 'd>,
    ) -> Owner<'app, 'd, D, Draining> {
        let retained = retained::Context::new(driver, self.retained);
        let mut app = token.pin_mut(self.stored).project().app;
        let _ = D::released(executor::raw::Released::new(app.as_mut()));
        let shutdown = executor::raw::Shutdown::new(app, turn, retained);
        let shutdown = D::shutdown(shutdown);
        let driver = shutdown.into_driver();
        let retained = retained::raw::Owner::from_driver(driver.driver_ref());
        Owner {
            stored: self.stored,
            retained,
            install: self.install,
            state: marker::PhantomData,
        }
    }
}

impl<'app, 'd: 'app, D> Owner<'app, 'd, D, Draining>
where
    D: executor::raw::App<'d>,
{
    /// # Safety
    /// Resume only the exact parent extracted after its child's native finish.
    /// Its original prepared plan and shutdown state remain owned by this backing.
    unsafe fn resume(
        stored: pin::Pin<&'app brand::Value<brand::Identity<'d>, Stored<D, D::ParkPlan>>>,
        token: &mut brand::Token<brand::Identity<'d>>,
        driver: driver::Reference<'d>,
    ) -> Self {
        let key: Key<'app, 'd, D> = Key {
            anchor: life::raw::Anchor::new(),
            driver: marker::PhantomData,
            owner: marker::PhantomData,
        };
        let _ = D::released(executor::raw::Released::new(
            token.pin_mut(stored).project().app,
        ));
        Self {
            stored,
            retained: retained::raw::Owner::from_driver(driver),
            install: life::Install::new(key, driver),
            state: marker::PhantomData,
        }
    }

    pub(crate) fn finish(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        finish: &mut finish::Finish<'_, 'd>,
    ) {
        let mut stored = token.pin_mut(self.stored);
        let finish =
            executor::raw::Finish::new(stored.as_mut().project().app, self.install, finish);
        let _ = D::finish(finish);
        // SAFETY: the exact pinned application has completed lifecycle
        // traversal. ManuallyDrop retains its backing, so dropping Stored
        // destroys both the application and its retained plan exactly once.
        unsafe { ptr::drop_in_place(stored.get_unchecked_mut()) };
    }
}

/// Exact stopped provider and its original plan after dependent native retirement.
pub(crate) struct Parent<'parent, 'd, H: executor::raw::Layout<'d>> {
    app: lending::raw::Borrowed<'parent, H>,
    park: Option<H::ParkPlan>,
}

impl<'parent, 'd, H: lending::raw::Resume<'d>> Parent<'parent, 'd, H> {
    pub(crate) fn with_owner<'life, Q, R>(
        self,
        core: &mut session::Core<'life, 'd, Q>,
        run: impl for<'app> FnOnce(
            &'app mut session::Core<'life, 'd, Q>,
            Owner<'app, 'd, lending::raw::Borrowed<'parent, H>, Draining>,
        ) -> R,
    ) -> R
    where
        'd: 'life,
    {
        let backing = pin::pin!(Backing::prepared(core.scope.token(), self.app, self.park));
        let driver = core.scope.driver_ref();
        // SAFETY: Parent consumes the same pinned provider and prepared plan
        // extracted after child native retirement. The execution scope ends
        // dependent construction state before returning this affine proof here.
        let owner = unsafe { Owner::resume(backing.as_ref().pin(), core.scope.token(), driver) };
        run(core, owner)
    }
}

impl<'app, 'parent: 'app, 'd: 'parent, H, T>
    Owner<'app, 'd, lending::raw::Bound<'parent, 'd, H, T>, Draining>
where
    H: lending::raw::Resume<'d>,
    T: executor::raw::App<'d>,
{
    pub(crate) fn finish_tail(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        finish: &mut finish::Finish<'_, 'd>,
    ) -> Parent<'parent, 'd, H> {
        let mut stored = token.pin_mut(self.stored);
        let plan = stored.as_mut().project().park.take();
        let call = executor::raw::Finish::new(stored.as_mut().project().app, self.install, finish);
        // SAFETY: this consumes the sole stopped owner after native quiescence.
        // The old backing is inaccessible; the child and its plan end here.
        // Only the original pinned provider and selected plan leave this frame.
        let (app, park) = unsafe { lending::raw::Bound::finish_tail(call, plan) };
        Parent { app, park }
    }
}

/// Copyable hot-path view borrowed from an affine application owner.
pub(crate) struct View<'app, 'd: 'app, D, S, Plan> {
    stored: pin::Pin<&'app brand::Value<brand::Identity<'d>, Stored<D, Plan>>>,
    retained: retained::raw::Owner<'app, 'd>,
    state: marker::PhantomData<fn(S) -> S>,
}

impl<'d, D, S, Plan> Copy for View<'_, 'd, D, S, Plan> {}

impl<'d, D, S, Plan> Clone for View<'_, 'd, D, S, Plan> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 'd: 'app, D, S, Plan> View<'app, 'd, D, S, Plan> {
    pub(crate) fn app<'borrow>(
        self,
        token: &'borrow brand::Token<brand::Identity<'d>>,
    ) -> pin::Pin<&'borrow D>
    where
        'app: 'borrow,
    {
        token.borrow_pin(self.stored).project_ref().app
    }

    pub(crate) fn context<'borrow>(
        self,
        driver: driver::Context<'borrow, 'd>,
    ) -> retained::Context<'borrow, 'app, 'd> {
        retained::Context::new(driver, self.retained)
    }
}

impl<'app, 'd: 'app, D, S, Plan> View<'app, 'd, D, S, Plan>
where
    D: executor::raw::App<'d, ParkPlan = Plan>,
{
    const fn new(owner: &'app Owner<'_, 'd, D, S>) -> Self {
        const {
            let pointer = mem::size_of::<usize>();
            assert!(mem::size_of::<Owner<'app, 'd, D, S>>() == pointer);
            assert!(mem::size_of::<Self>() == pointer);
        }
        Self {
            stored: owner.stored,
            retained: owner.retained,
            state: marker::PhantomData,
        }
    }

    pub(crate) fn dispatch<'turn>(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        event: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let ctx = self.context(driver.reborrow());
        let app = token.pin_mut(self.stored).project().app;
        let call = executor::raw::Dispatch::new(app, event, turn, ctx);
        match D::dispatch(call) {
            ops::ControlFlow::Continue(()) => ops::ControlFlow::Continue(()),
            ops::ControlFlow::Break(event) => driver.retire(event),
        }
    }

    pub(crate) fn activate<'turn>(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        target: route::Erased<'d, kind::Any>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let ctx = self.context(driver.reborrow());
        let wake = executor::raw::Activate::new(target, turn, ctx);
        wake.wake::<D>(token.pin_mut(self.stored).project().app);
    }

    pub(crate) fn pre_park<'turn>(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let ctx = self.context(driver.reborrow());
        let park = executor::raw::Park::new(turn, ctx);
        let stored = token.pin_mut(self.stored).project();
        park.run::<D>(stored.app, stored.park);
    }

    pub(crate) fn progress<'turn>(
        self,
        token: &brand::Token<brand::Identity<'d>>,
        region: &(
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        clock: clock::Clock<'turn, 'd>,
    ) -> schedule::Progress<'d> {
        let view = executor::raw::Progress::new(region, clock);
        D::progress(self.app(token), view)
    }

    pub(crate) fn stopping<'turn>(
        self,
        token: &brand::Token<brand::Identity<'d>>,
        region: &(
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        clock: clock::Clock<'turn, 'd>,
    ) -> schedule::Progress<'d> {
        let view = executor::raw::Progress::new(region, clock);
        D::stopping(self.app(token), view)
    }

    pub(crate) fn begin_drain<'driver, 'turn>(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) where
        D: Drain<'d>,
    {
        let mut ctx = self.context(driver);
        D::begin(token.pin_mut(self.stored).project().app, turn, &mut ctx);
    }

    pub(crate) fn draining(
        self,
        token: &brand::Token<brand::Identity<'d>>,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d>
    where
        D: Drain<'d>,
    {
        <D as Drain<'d>>::progress(self.app(token), region)
    }

    pub(crate) fn force_drain<'driver, 'turn>(
        self,
        token: &mut brand::Token<brand::Identity<'d>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) where
        D: Drain<'d>,
    {
        let mut ctx = self.context(driver);
        D::force(token.pin_mut(self.stored).project().app, turn, &mut ctx);
    }
}
