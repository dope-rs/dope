use std::mem;

use dope_core::{
    driver::{self, schedule},
    io,
};

/// Completion authority for one exact retained root and scheduler turn.
pub struct Call<'driver, 'turn, 'd> {
    event: io::Event<'d>,
    turn: schedule::Turn<'turn, 'd>,
    driver: driver::Context<'driver, 'd>,
}

impl<'driver, 'turn, 'd> Call<'driver, 'turn, 'd> {
    /// Binds one completion to the installed root context that must consume it.
    pub const fn new(
        event: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) -> Self {
        Self {
            event,
            turn,
            driver,
        }
    }

    pub(in crate::app::dispatch) fn into_event(self) -> io::Event<'d> {
        self.event
    }

    pub(in crate::app::dispatch) fn into_drive(
        self,
    ) -> (io::Event<'d>, driver::Context<'driver, 'd>) {
        (self.event, self.driver)
    }

    pub(in crate::app::dispatch) fn into_turn(
        self,
    ) -> (
        io::Event<'d>,
        schedule::Turn<'turn, 'd>,
        driver::Context<'driver, 'd>,
    ) {
        (self.event, self.turn, self.driver)
    }
}

const fn assert_layout() {
    type Exact<'a> = Call<'a, 'a, 'a>;
    assert!(
        mem::size_of::<Exact<'static>>()
            == mem::size_of::<io::Event<'static>>()
                + mem::size_of::<schedule::Turn<'static, 'static>>()
                + mem::size_of::<driver::Context<'static, 'static>>()
    );
    assert!(mem::align_of::<Exact<'static>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();
