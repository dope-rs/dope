//! Runtime-created calls bind every lifecycle operation to one exact pinned
//! application instance.
//!
//! A call cannot be retagged to another application type:
//!
//! ```compile_fail,E0308
//! use dope_runtime::executor::raw::Install;
//!
//! fn retag<'pin, 'app, 'd: 'app, A, B>(
//!     call: Install<'pin, 'app, 'd, A>,
//! ) -> Install<'pin, 'app, 'd, B> {
//!     call
//! }
//! ```
//!
//! Call construction remains exclusive to the runtime:
//!
//! ```compile_fail,E0624
//! use dope_runtime::executor::raw::Install;
//!
//! let _ = Install::<()>::new(todo!(), todo!());
//! ```

use std::{marker, ops, pin};

use dope_core::{
    driver::{self, retained, route, schedule},
    io,
};
use o3::cell::{brand, region};

mod finish;
mod install;
mod pending;

pub(crate) use finish::FinishRoot;
pub(crate) use install::InstallRoot;
pub use pending::Pending;

/// Exact-root shutdown authority for one installed application.
#[must_use]
pub struct ShutdownRoot<'a, 'app, 'd: 'app, D> {
    driver: retained::Context<'a, 'app, 'd>,
    _app: marker::PhantomData<*mut &'app ()>,
    _owner: marker::PhantomData<*mut D>,
}

impl<'a, 'app, 'd: 'app, D> ShutdownRoot<'a, 'app, 'd, D> {
    /// # Safety
    /// `app` must be the exact pinned application retained by this lifecycle.
    pub(crate) unsafe fn new(
        driver: retained::Context<'a, 'app, 'd>,
        _app: pin::Pin<&'app brand::Value<'d, D>>,
    ) -> Self {
        Self {
            driver,
            _app: marker::PhantomData,
            _owner: marker::PhantomData,
        }
    }

    pub fn driver(&mut self) -> &mut retained::Context<'a, 'app, 'd> {
        &mut self.driver
    }

    pub fn pending(&mut self) -> Pending<'app, 'd, D> {
        Pending::new()
    }
}

const _: () = assert!(
    std::mem::size_of::<ShutdownRoot<'static, 'static, 'static, ()>>()
        == std::mem::size_of::<driver::Context<'static, 'static>>()
);

/// Exact pinned application installation call.
/// Only the runtime constructs it; retaining the pin prevents authority from
/// being retagged to another value of the same type.
#[doc(hidden)]
#[must_use]
pub struct Install<'pin, 'app, 'd: 'app, D> {
    app: pin::Pin<&'pin mut D>,
    install: InstallRoot<'app, 'd, D>,
}

impl<'pin, 'app, 'd: 'app, D> Install<'pin, 'app, 'd, D> {
    pub(crate) fn new(app: pin::Pin<&'pin mut D>, install: InstallRoot<'app, 'd, D>) -> Self {
        Self { app, install }
    }

    /// # Safety
    /// Every installed retained owner reached through the returned pin must
    /// be visited during shutdown and finish while the application stays pinned.
    pub unsafe fn into_parts_unchecked(self) -> (pin::Pin<&'pin mut D>, InstallRoot<'app, 'd, D>) {
        (self.app, self.install)
    }
}

/// Exact pinned application event-dispatch call.
#[doc(hidden)]
#[must_use]
pub struct Dispatch<'pin, 'driver, 'turn, 'app, 'd: 'app, D> {
    app: pin::Pin<&'pin mut D>,
    event: io::Event<'d>,
    turn: schedule::Turn<'turn, 'd>,
    driver: retained::Context<'driver, 'app, 'd>,
}

impl<'pin, 'driver, 'turn, 'app, 'd: 'app, D> Dispatch<'pin, 'driver, 'turn, 'app, 'd, D> {
    pub(crate) fn new(
        app: pin::Pin<&'pin mut D>,
        event: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'app, 'd>,
    ) -> Self {
        Self {
            app,
            event,
            turn,
            driver,
        }
    }

    /// # Safety
    /// The caller must preserve the exact installed-owner lifecycle and must
    /// return a deferred event unchanged before observable dispatch effects.
    pub unsafe fn into_parts_unchecked(
        self,
    ) -> (
        pin::Pin<&'pin mut D>,
        io::Event<'d>,
        schedule::Turn<'turn, 'd>,
        retained::Context<'driver, 'app, 'd>,
    ) {
        (self.app, self.event, self.turn, self.driver)
    }

    pub fn consume(self) -> ops::ControlFlow<io::Event<'d>> {
        let Self {
            app: _,
            event,
            turn: _,
            driver: _,
        } = self;
        let _ = event;
        ops::ControlFlow::Continue(())
    }

    pub fn defer(self) -> ops::ControlFlow<io::Event<'d>> {
        let Self {
            app: _,
            event,
            turn: _,
            driver: _,
        } = self;
        ops::ControlFlow::Break(event)
    }
}

/// Exact pinned application activation call.
#[doc(hidden)]
#[must_use]
pub struct Activate<'pin, 'driver, 'turn, 'app, 'd: 'app, D> {
    app: pin::Pin<&'pin mut D>,
    target: route::Token,
    turn: schedule::Turn<'turn, 'd>,
    driver: retained::Context<'driver, 'app, 'd>,
}

impl<'pin, 'driver, 'turn, 'app, 'd: 'app, D> Activate<'pin, 'driver, 'turn, 'app, 'd, D> {
    pub(crate) fn new(
        app: pin::Pin<&'pin mut D>,
        target: route::Token,
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'app, 'd>,
    ) -> Self {
        Self {
            app,
            target,
            turn,
            driver,
        }
    }

    /// # Safety
    /// The returned application and retained context must only drive owners
    /// structurally pinned beneath this exact installed application.
    pub unsafe fn into_parts_unchecked(
        self,
    ) -> (
        pin::Pin<&'pin mut D>,
        route::Token,
        schedule::Turn<'turn, 'd>,
        retained::Context<'driver, 'app, 'd>,
    ) {
        (self.app, self.target, self.turn, self.driver)
    }
}

/// Exact pinned application pre-park call.
#[doc(hidden)]
#[must_use]
pub struct PrePark<'pin, 'driver, 'turn, 'app, 'd: 'app, D> {
    app: pin::Pin<&'pin mut D>,
    turn: schedule::Turn<'turn, 'd>,
    driver: retained::Context<'driver, 'app, 'd>,
}

impl<'pin, 'driver, 'turn, 'app, 'd: 'app, D> PrePark<'pin, 'driver, 'turn, 'app, 'd, D> {
    pub(crate) fn new(
        app: pin::Pin<&'pin mut D>,
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'app, 'd>,
    ) -> Self {
        Self { app, turn, driver }
    }

    /// # Safety
    /// The returned application and retained context must only drive owners
    /// structurally pinned beneath this exact installed application.
    pub unsafe fn into_parts_unchecked(
        self,
    ) -> (
        pin::Pin<&'pin mut D>,
        schedule::Turn<'turn, 'd>,
        retained::Context<'driver, 'app, 'd>,
    ) {
        (self.app, self.turn, self.driver)
    }
}

/// Exact pinned application progress query.
#[doc(hidden)]
#[must_use]
pub struct Progress<'pin, 'region, 'd, D> {
    app: pin::Pin<&'pin D>,
    region: &'region region::Token<'d>,
}

impl<'pin, 'region, 'd, D> Progress<'pin, 'region, 'd, D> {
    pub(crate) fn new(app: pin::Pin<&'pin D>, region: &'region region::Token<'d>) -> Self {
        Self { app, region }
    }

    /// # Safety
    /// The returned pin must not be used to bypass the lifecycle traversal of
    /// the exact installed application.
    pub unsafe fn into_parts_unchecked(self) -> (pin::Pin<&'pin D>, &'region region::Token<'d>) {
        (self.app, self.region)
    }
}

/// Exact pinned application shutdown call.
#[doc(hidden)]
#[must_use]
pub struct Shutdown<'pin, 'driver, 'turn, 'app, 'd: 'app, D> {
    app: pin::Pin<&'pin mut D>,
    turn: schedule::Turn<'turn, 'd>,
    shutdown: ShutdownRoot<'driver, 'app, 'd, D>,
}

impl<'pin, 'driver, 'turn, 'app, 'd: 'app, D> Shutdown<'pin, 'driver, 'turn, 'app, 'd, D> {
    pub(crate) fn new(
        app: pin::Pin<&'pin mut D>,
        turn: schedule::Turn<'turn, 'd>,
        shutdown: ShutdownRoot<'driver, 'app, 'd, D>,
    ) -> Self {
        Self {
            app,
            turn,
            shutdown,
        }
    }

    /// # Safety
    /// Every retained owner installed beneath the returned pin must be
    /// visited exactly once before the returned shutdown proof is completed.
    pub unsafe fn into_parts_unchecked(
        self,
    ) -> (
        pin::Pin<&'pin mut D>,
        schedule::Turn<'turn, 'd>,
        ShutdownRoot<'driver, 'app, 'd, D>,
    ) {
        (self.app, self.turn, self.shutdown)
    }

    /// Completes shutdown for an application that installed no retained
    /// owners and never used unchecked call access.
    pub fn complete(mut self) -> Pending<'app, 'd, D> {
        self.shutdown.pending()
    }
}

/// Exact pinned application finalization call.
#[doc(hidden)]
#[must_use]
pub struct Finish<'pin, 'finalization, 'app, 'd: 'app, D> {
    app: pin::Pin<&'pin mut D>,
    finish: FinishRoot<'finalization, 'app, 'd, D>,
}

impl<'pin, 'finalization, 'app, 'd: 'app, D> Finish<'pin, 'finalization, 'app, 'd, D> {
    pub(crate) fn new(
        app: pin::Pin<&'pin mut D>,
        finish: FinishRoot<'finalization, 'app, 'd, D>,
    ) -> Self {
        Self { app, finish }
    }

    /// # Safety
    /// Every retained owner installed beneath the returned pin must be
    /// finalized exactly once through the returned context.
    pub unsafe fn into_parts_unchecked(
        self,
    ) -> (
        pin::Pin<&'pin mut D>,
        FinishRoot<'finalization, 'app, 'd, D>,
    ) {
        (self.app, self.finish)
    }
}
