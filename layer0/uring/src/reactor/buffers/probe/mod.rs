use dope_sys::{io::Close, memory};
pub(crate) use payload::Payload;

use crate::reactor::buffers::{self, Cleanup, Producer};

mod payload;

#[derive(Debug)]
pub struct Canary<L: memory::raw::Canary> {
    pub(super) producer: Producer<L>,
    pub(super) context: buffers::raw::Context<L>,
}

impl<L: memory::raw::Canary> Close for Canary<L> {
    type State = Cleanup<L>;

    fn try_close(self) -> Result<(), Cleanup<L>> {
        let Self { producer, context } = self;
        Cleanup::close(producer, context)
    }
}
