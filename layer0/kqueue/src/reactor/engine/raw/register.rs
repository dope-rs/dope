use dope_sys::io::reactor::ready::{Poll as _, store::State as _};

use crate::reactor::{
    Engine,
    engine::{engine, ready, register},
};

impl<C: ready::Driver> engine::raw::Register for Engine<C> {
    unsafe fn register(&mut self, source: register::Source<'_>) -> Result<(), register::Error> {
        self.state
            .view()
            .poll
            .arm_shutdown(source.fd, C::shutdown(source.interest))
            .map_err(register::Error::Register)
    }
}
