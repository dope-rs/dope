use std::{cell, io, mem, process};

use crate::driver::{self, lifecycle, ops, route};

pub(crate) struct Routes {
    live: [u64; route::CAPACITY / u64::BITS as usize],
}

impl Routes {
    pub(crate) const fn new() -> Self {
        Self {
            live: [0; route::CAPACITY / u64::BITS as usize],
        }
    }

    pub(crate) fn reserve(&mut self, id: u8) -> bool {
        let word = id as usize / 64;
        let mask = 1u64 << (id % 64);
        if self.live[word] & mask != 0 {
            return false;
        }
        self.live[word] |= mask;
        true
    }

    pub(crate) fn release(&mut self, id: u8) {
        self.live[id as usize / 64] &= !(1u64 << (id % 64));
    }
}

pub struct Route<'d, const ID: u8> {
    driver: driver::Reference<'d>,
    state: cell::Cell<State>,
}

#[doc(hidden)]
#[repr(transparent)]
pub struct StorageRoute<'d, const ID: u8> {
    route: Route<'d, ID>,
}

const _: () =
    assert!(mem::size_of::<StorageRoute<'static, 0>>() == mem::size_of::<Route<'static, 0>>());

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Staged,
    Active,
    Retired,
}

/// A route reserved by non-submitting storage construction but not yet bound
/// to an operation owner.
///
/// ```compile_fail
/// use dope_core::driver::lifecycle::routing::Reserved;
///
/// fn rebrand<'a, 'b>(route: Reserved<'a, 7>) -> Reserved<'b, 7> {
///     route
/// }
/// ```
///
/// ```compile_fail
/// use dope_core::driver::lifecycle::routing::Reserved;
///
/// fn bind_twice<'d>(route: Reserved<'d, 7>) {
///     let _staged = route.bind();
///     let _duplicate = route.bind();
/// }
/// ```
///
/// Installation proof and route must carry the exact same invariant driver
/// lifetime:
///
/// ```compile_fail
/// use dope_core::driver::lifecycle::{Install, routing::Route};
///
/// fn cross_driver<'a, 'b>(
///     route: &Route<'a, 7>,
///     install: &mut Install<'_, 'b>,
/// ) {
///     route.install(install);
/// }
/// ```
#[doc(hidden)]
pub struct Reserved<'d, const ID: u8> {
    driver: Option<driver::Reference<'d>>,
}

impl<'d, const ID: u8> Reserved<'d, ID> {
    pub(crate) fn reserve_allowed(driver: &mut driver::Context<'_, 'd>) -> io::Result<Self> {
        if !ops::Control::reserve_route(driver, ID) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "dope: route already used",
            ));
        }
        Ok(Self {
            driver: Some(driver.driver_ref()),
        })
    }

    /// Consumes this linear reservation into rollback-safe staged ownership.
    /// Only a pinned runtime installation can activate it.
    pub fn bind(mut self) -> Route<'d, ID> {
        let Some(driver) = self.driver.take() else {
            process::abort();
        };
        Route {
            driver,
            state: cell::Cell::new(State::Staged),
        }
    }

    /// Moves this linear reservation into a reinstallable storage route.
    pub fn bind_storage(self) -> StorageRoute<'d, ID> {
        StorageRoute { route: self.bind() }
    }

    fn release(mut self, driver: &mut driver::Context<'_, 'd>) {
        if self.driver.take().is_some() {
            ops::Control::release_route(driver, ID);
        }
    }
}

impl<const ID: u8> Drop for Reserved<'_, ID> {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            driver.maintenance().defer_route(ID);
        }
    }
}

#[doc(hidden)]
pub struct RouteReservation<'a, 'c, 'd, const ID: u8> {
    route: Option<Reserved<'d, ID>>,
    driver: &'a mut driver::Context<'c, 'd>,
}

impl<'d, const ID: u8> Route<'d, ID> {
    #[doc(hidden)]
    pub fn reserve_transaction<'a, 'c>(
        driver: &'a mut driver::Context<'c, 'd>,
    ) -> io::Result<RouteReservation<'a, 'c, 'd, ID>> {
        use crate::driver::route::FRAMEWORK;

        if ID == FRAMEWORK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: reserved route",
            ));
        }
        let route = Reserved::reserve_allowed(driver)?;
        Ok(RouteReservation {
            route: Some(route),
            driver,
        })
    }

    pub fn driver(&self) -> driver::Reference<'d> {
        if self.state.get() == State::Retired {
            process::abort();
        }
        self.driver
    }

    /// Activates this staged route under a pinned application installation.
    pub fn install(&self, _install: &mut lifecycle::Install<'_, 'd>) {
        if self.state.replace(State::Active) != State::Staged {
            process::abort();
        }
    }

    pub fn release(self, driver: &mut driver::Context<'_, 'd>) {
        if self.state.replace(State::Retired) != State::Retired {
            ops::Control::release_route(driver, ID);
        }
    }

    pub fn finish(&self, driver: &mut driver::Context<'_, 'd>) {
        if self.state.replace(State::Retired) == State::Retired {
            return;
        }
        ops::Control::release_route(driver, ID);
    }

    fn stage(&self) {
        if self.state.replace(State::Staged) != State::Active {
            process::abort();
        }
    }

    #[doc(hidden)]
    pub fn assert_droppable(&self) {
        if self.state.get() == State::Active {
            process::abort();
        }
    }
}

impl<'d, const ID: u8> StorageRoute<'d, ID> {
    pub fn install(&self, install: &mut lifecycle::Install<'_, 'd>) {
        self.route.install(install);
    }

    pub(crate) fn stage(&self) {
        self.route.stage();
    }
}

impl<const ID: u8> Drop for Route<'_, ID> {
    fn drop(&mut self) {
        match self.state.replace(State::Retired) {
            State::Staged => self.driver.maintenance().defer_route(ID),
            State::Active => process::abort(),
            State::Retired => {}
        }
    }
}

impl<'c, 'd, const ID: u8> RouteReservation<'_, 'c, 'd, ID> {
    pub fn driver(&mut self) -> &mut driver::Context<'c, 'd> {
        self.driver
    }

    /// Commits the transaction into a staged, rollback-safe lifecycle owner.
    pub fn commit(mut self) -> Route<'d, ID> {
        let Some(route) = self.route.take() else {
            process::abort();
        };
        route.bind()
    }
}

impl<const ID: u8> Drop for RouteReservation<'_, '_, '_, ID> {
    fn drop(&mut self) {
        if let Some(route) = self.route.take() {
            route.release(self.driver);
        }
    }
}
