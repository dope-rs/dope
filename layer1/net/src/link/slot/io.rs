use dope_core::driver::{route::Identity, schedule::ready};

use crate::{link::slot, wire};

pub struct Io<'a, 'd, ID: Identity, W: wire::Spec> {
    engine: &'a slot::Engine<'d, ID, W>,
}

impl<'a, 'd, ID: Identity, W: wire::Spec> Io<'a, 'd, ID, W> {
    pub(super) fn new(engine: &'a slot::Engine<'d, ID, W>) -> Self {
        Self { engine }
    }

    /// Returns the generation-checked readiness target for this connection.
    /// It retains the driver lifetime and becomes a no-op once its ready slot
    /// is released.
    pub fn wake_target(&self) -> ready::Target<'d> {
        self.engine.ready_handle().target()
    }

    /// Marks this connection's lifecycle as closing.
    pub fn close(&self) {
        self.engine.operations.lifecycle.close();
    }

    pub(in crate::link) fn ready_key(&self) -> ready::Key<'d> {
        self.engine.ready_handle().key()
    }

    pub(in crate::link) fn ready_handle(&self) -> ready::Handle<'d> {
        self.engine.ready_handle()
    }
}
