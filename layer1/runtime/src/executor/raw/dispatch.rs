use std::{mem, ops, pin};

use dope_core::{
    driver::{retained, route::Route, schedule},
    io,
};

use crate::executor;

/// Event dispatch authority for one exact retained root and scheduler turn.
#[must_use]
pub struct Dispatch<'app, 'driver, 'turn, 'root, 'd: 'root, R> {
    app: pin::Pin<&'app mut R>,
    drive: executor::raw::Drive<'driver, 'turn, 'root, 'd>,
    event: io::Event<'d>,
}

impl<'app, 'driver, 'turn, 'root, 'd: 'root, R> Dispatch<'app, 'driver, 'turn, 'root, 'd, R> {
    /// Binds one event to its exact pinned root and retained driver authority.
    pub const fn new(
        app: pin::Pin<&'app mut R>,
        event: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'root, 'd>,
    ) -> Self {
        Self {
            app,
            drive: executor::raw::Drive::new(turn, driver),
            event,
        }
    }

    pub fn route(&self) -> Route {
        self.event.route()
    }

    pub fn map(
        mut self,
        map: impl FnOnce(
            pin::Pin<&mut R>,
            io::Event<'d>,
            schedule::Turn<'_, 'd>,
            retained::Context<'_, 'root, 'd>,
        ) -> ops::ControlFlow<io::Event<'d>>,
    ) -> ops::ControlFlow<Self> {
        let (turn, driver) = self.drive.reborrow().into_parts();
        match map(self.app.as_mut(), self.event, turn, driver) {
            ops::ControlFlow::Continue(()) => ops::ControlFlow::Continue(()),
            ops::ControlFlow::Break(event) => {
                self.event = event;
                ops::ControlFlow::Break(self)
            }
        }
    }

    pub fn into_event(self) -> io::Event<'d> {
        self.event
    }

    pub fn into_parts(self) -> (pin::Pin<&'app mut R>, io::Event<'d>) {
        (self.app, self.event)
    }
}

const fn assert_layout() {
    type Call<'a> = Dispatch<'a, 'a, 'a, 'a, 'a, ()>;
    type Drive<'a> = executor::raw::Drive<'a, 'a, 'a, 'a>;

    assert!(
        mem::size_of::<Call<'static>>()
            == mem::size_of::<Drive<'static>>()
                + mem::size_of::<io::Event<'static>>()
                + mem::size_of::<pin::Pin<&'static mut ()>>()
    );
    assert!(mem::align_of::<Call<'static>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();
