use std::{num, ptr};

use dope_sys::{io::Close, memory, memory::buffer::provided::Producer as _};
pub(crate) use payload::Payload;

use crate::reactor::buffers::{self, Cleanup, Producer};

mod payload;

#[derive(Debug)]
pub struct Canary<L: memory::raw::Canary> {
    pub(super) producer: Producer<L>,
    pub(super) context: buffers::raw::Context<L>,
}

impl<L: memory::raw::Canary> Canary<L> {
    pub(crate) fn registration(&self) -> (ptr::NonNull<buffers::Entry>, num::NonZero<u16>) {
        self.producer.registration()
    }
}

impl<L: memory::raw::Canary> Close for Canary<L> {
    type State = Cleanup<L>;

    fn try_close(self) -> Result<(), Cleanup<L>> {
        let Self { producer, context } = self;
        Cleanup::close(producer, context)
    }
}
