use std::{mem, pin};

use dope_core::driver::{
    retained,
    route::{self, kind},
    schedule,
};

use crate::executor;

/// Route activation authority for one exact retained root and scheduler turn.
#[must_use]
pub struct Activate<'driver, 'turn, 'root, 'd: 'root> {
    drive: executor::raw::Drive<'driver, 'turn, 'root, 'd>,
    target: route::Erased<'d, kind::Any>,
}

impl<'driver, 'turn, 'root, 'd: 'root> Activate<'driver, 'turn, 'root, 'd> {
    pub(crate) const fn new(
        target: route::Erased<'d, kind::Any>,
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'root, 'd>,
    ) -> Self {
        Self {
            drive: executor::raw::Drive::new(turn, driver),
            target,
        }
    }

    pub const fn target(&self) -> route::Erased<'d, kind::Any> {
        self.target
    }

    pub fn reborrow(&mut self) -> Activate<'_, '_, 'root, 'd> {
        Activate {
            drive: self.drive.reborrow(),
            target: self.target,
        }
    }

    pub(crate) fn wake<R>(self, app: pin::Pin<&mut R>)
    where
        R: executor::raw::App<'d>,
    {
        if !R::WAKE_ROUTES.has_route(self.target.route()) {
            return;
        }
        // SAFETY: the session view admits only a target declared by the
        // activation route set and supplies its exact installed pinned root.
        unsafe { R::activate(app, self) };
    }

    pub fn into_driver(
        self,
    ) -> (
        route::Erased<'d, kind::Any>,
        retained::Context<'driver, 'root, 'd>,
    ) {
        (self.target, self.drive.into_driver())
    }

    pub fn map<R>(
        self,
        map: impl FnOnce(
            route::Erased<'d, kind::Any>,
            schedule::Turn<'turn, 'd>,
            retained::Context<'driver, 'root, 'd>,
        ) -> R,
    ) -> R {
        let (turn, driver) = self.drive.into_parts();
        map(self.target, turn, driver)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Activate<'static, 'static, 'static, 'static>>()
            == mem::size_of::<executor::raw::Drive<'static, 'static, 'static, 'static>>()
                + mem::size_of::<route::Erased<'static, kind::Any>>()
    );
    assert!(
        mem::align_of::<Activate<'static, 'static, 'static, 'static>>() == mem::align_of::<usize>()
    );
};
