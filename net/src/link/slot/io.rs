use dope_core::driver::{self, schedule::ready};

use crate::link;

pub struct Io<'a, 'd> {
    engine: &'a link::Engine<'d>,
}

impl<'a, 'd> Io<'a, 'd> {
    pub(super) fn new(engine: &'a link::Engine<'d>) -> Self {
        Self { engine }
    }

    /// Returns the generation-checked readiness target for this connection.
    /// It retains the driver lifetime and becomes a no-op once its ready slot
    /// is released.
    pub fn wake_target(&self) -> ready::Target<'d> {
        self.engine.ready_handle().target()
    }

    #[doc(hidden)]
    pub fn ready_key(&self) -> ready::Key<'d> {
        self.engine.ready_handle().key()
    }

    #[doc(hidden)]
    pub fn ready_handle(&self) -> ready::Handle<'d> {
        self.engine.ready_handle()
    }

    #[doc(hidden)]
    pub fn driver(&self) -> driver::Reference<'d> {
        self.engine.driver()
    }
}
