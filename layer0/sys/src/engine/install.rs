use std::os::fd;

/// Installs an owned descriptor in its reserved native table slot.
pub trait Install<O> {
    type Handle: fd::AsFd;
    type Error;

    fn install(&mut self, slot: O, handle: Self::Handle) -> Result<(), Self::Error>;
}
