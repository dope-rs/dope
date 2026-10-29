use std::mem;

use dope_core::{
    driver::{retained, schedule},
    io,
};

/// Completion authority for one exact retained root and scheduler turn.
pub struct Call<'driver, 'turn, 'root, 'd: 'root> {
    event: io::Event<'d>,
    turn: schedule::Turn<'turn, 'd>,
    driver: retained::Context<'driver, 'root, 'd>,
}

impl<'driver, 'turn, 'root, 'd: 'root> Call<'driver, 'turn, 'root, 'd> {
    /// Binds one completion to the installed root context that must consume it.
    pub const fn new(
        event: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'root, 'd>,
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
    ) -> (io::Event<'d>, retained::Context<'driver, 'root, 'd>) {
        (self.event, self.driver)
    }

    pub(in crate::app::dispatch) fn into_turn(
        self,
    ) -> (
        io::Event<'d>,
        schedule::Turn<'turn, 'd>,
        retained::Context<'driver, 'root, 'd>,
    ) {
        (self.event, self.turn, self.driver)
    }
}

const fn assert_layout() {
    type Exact<'a> = Call<'a, 'a, 'a, 'a>;
    assert!(
        mem::size_of::<Exact<'static>>()
            == mem::size_of::<io::Event<'static>>()
                + mem::size_of::<schedule::Turn<'static, 'static>>()
                + mem::size_of::<retained::Context<'static, 'static, 'static>>()
    );
    assert!(mem::align_of::<Exact<'static>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();
