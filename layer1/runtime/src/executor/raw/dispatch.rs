use std::{mem, pin};

use dope_core::{
    driver::{self, schedule},
    io,
};

use crate::executor;

/// Event dispatch authority for one exact retained root and scheduler turn.
#[must_use]
pub struct Dispatch<'app, 'driver, 'turn, 'd, R> {
    app: pin::Pin<&'app mut R>,
    drive: executor::raw::Drive<'driver, 'turn, 'd>,
    event: io::Event<'d>,
}

impl<'app, 'driver, 'turn, 'd, R> Dispatch<'app, 'driver, 'turn, 'd, R> {
    /// Binds one event to its exact pinned root and retained driver authority.
    pub const fn new(
        app: pin::Pin<&'app mut R>,
        event: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) -> Self {
        Self {
            app,
            drive: executor::raw::Drive { turn, driver },
            event,
        }
    }

    pub fn into_event(self) -> io::Event<'d> {
        self.event
    }

    pub fn into_parts(
        self,
    ) -> (
        pin::Pin<&'app mut R>,
        io::Event<'d>,
        schedule::Turn<'turn, 'd>,
        driver::Context<'driver, 'd>,
    ) {
        (self.app, self.event, self.drive.turn, self.drive.driver)
    }
}

const fn assert_layout() {
    type Call<'a> = Dispatch<'a, 'a, 'a, 'a, ()>;
    type Drive<'a> = executor::raw::Drive<'a, 'a, 'a>;

    assert!(
        mem::size_of::<Call<'static>>()
            == mem::size_of::<Drive<'static>>()
                + mem::size_of::<io::Event<'static>>()
                + mem::size_of::<pin::Pin<&'static mut ()>>()
    );
    assert!(mem::align_of::<Call<'static>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();
