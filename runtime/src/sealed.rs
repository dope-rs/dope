//! Runtime-owned proofs that close raw driver and dispatcher obligations.

use std::{mem, ops, pin};

use dope_core::{
    driver::{self, lifecycle::quiesce, retained, route, schedule},
    io,
};
use o3::cell::{brand, region};

use crate::executor;

pub(crate) struct Owner(quiesce::Lease);

const _: () = assert!(mem::size_of::<Owner>() == 0);

pub(crate) struct Installed<'app, 'd: 'app, D> {
    app: pin::Pin<&'app brand::Value<'d, D>>,
    retained: retained::raw::Owner<'app, 'd>,
}

impl<D> Copy for Installed<'_, '_, D> {}

impl<D> Clone for Installed<'_, '_, D> {
    fn clone(&self) -> Self {
        *self
    }
}

const _: () = assert!(
    mem::size_of::<Installed<'static, 'static, ()>>()
        == mem::size_of::<pin::Pin<&'static brand::Value<'static, ()>>>()
);

const _: () = assert!(
    mem::align_of::<Installed<'static, 'static, ()>>()
        == mem::align_of::<pin::Pin<&'static brand::Value<'static, ()>>>()
);

impl Owner {
    pub(crate) fn acquire() -> Self {
        // SAFETY: SessionCore completes final quiescence before storage release.
        let owner = unsafe { quiesce::raw::Owner::new() };
        Self(quiesce::Lease::new(owner))
    }

    pub(crate) fn into_inner(self) -> quiesce::Lease {
        self.0
    }
}

impl<'app, 'd: 'app, D> Installed<'app, 'd, D>
where
    D: executor::Application<'d>,
{
    pub(crate) fn install(
        app: pin::Pin<&'app brand::Value<'d, D>>,
        token: &mut brand::Token<'d>,
    ) -> Self {
        // SAFETY: the returned capability is created only after installation
        // of this exact pinned dispatcher.
        let install = unsafe { executor::raw::InstallRoot::new(app) };
        D::install(executor::raw::Install::new(
            app.borrow_pin_mut(token),
            install,
        ));
        // SAFETY: Installed retains this exact pinned dispatcher through
        // shutdown, completion drain, and finish.
        let retained = unsafe { retained::raw::Owner::new(app) };
        Self { app, retained }
    }

    pub(crate) fn borrow_pin<'borrow>(
        self,
        token: &'borrow brand::Token<'d>,
    ) -> pin::Pin<&'borrow D>
    where
        'app: 'borrow,
    {
        self.app.borrow_pin(token)
    }

    pub(crate) fn reborrow<'borrow>(self) -> Installed<'borrow, 'd, D>
    where
        'app: 'borrow,
    {
        let app: pin::Pin<&'borrow brand::Value<'d, D>> = self.app;
        Installed {
            app,
            retained: self.retained,
        }
    }

    pub(crate) fn retained_context<'borrow>(
        self,
        driver: driver::Context<'borrow, 'd>,
    ) -> retained::Context<'borrow, 'app, 'd> {
        retained::Context::new(driver, self.retained)
    }

    pub(crate) fn dispatch<'turn>(
        self,
        token: &mut brand::Token<'d>,
        event: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let retained = self.retained_context(driver.reborrow());
        D::dispatch(executor::raw::Dispatch::new(
            self.app.borrow_pin_mut(token),
            event,
            turn,
            retained,
        ))
    }

    pub(crate) fn activate<'turn>(
        self,
        token: &mut brand::Token<'d>,
        target: route::Token,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let retained = self.retained_context(driver.reborrow());
        D::activate(executor::raw::Activate::new(
            self.app.borrow_pin_mut(token),
            target,
            turn,
            retained,
        ));
    }

    pub(crate) fn pre_park<'turn>(
        self,
        token: &mut brand::Token<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let retained = self.retained_context(driver.reborrow());
        D::pre_park(executor::raw::PrePark::new(
            self.app.borrow_pin_mut(token),
            turn,
            retained,
        ));
    }

    pub(crate) fn progress(
        self,
        token: &brand::Token<'d>,
        region: &region::Token<'d>,
    ) -> schedule::Progress<'d> {
        D::progress(executor::raw::Progress::new(
            self.app.borrow_pin(token),
            region,
        ))
    }

    pub(crate) fn shutdown_progress(
        self,
        token: &brand::Token<'d>,
        region: &region::Token<'d>,
    ) -> schedule::Progress<'d> {
        D::shutdown_progress(executor::raw::Progress::new(
            self.app.borrow_pin(token),
            region,
        ))
    }

    pub(crate) fn shutdown<'a, 'turn>(
        self,
        token: &mut brand::Token<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'a, 'd>,
    ) -> executor::raw::Pending<'app, 'd, D> {
        let retained = self.retained_context(driver);
        // SAFETY: Installed retains the exact application passed here through
        // the shutdown and finish sequence.
        let shutdown = unsafe { executor::raw::ShutdownRoot::new(retained, self.app) };
        D::shutdown(executor::raw::Shutdown::new(
            self.app.borrow_pin_mut(token),
            turn,
            shutdown,
        ))
    }

    pub(crate) fn finish(
        self,
        token: &mut brand::Token<'d>,
        finish: executor::raw::FinishRoot<'_, 'app, 'd, D>,
    ) {
        D::finish(executor::raw::Finish::new(
            self.app.borrow_pin_mut(token),
            finish,
        ));
    }
}
