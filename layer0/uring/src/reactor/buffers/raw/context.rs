use std::slice;

use dope_sys::{
    io::Close,
    memory,
    memory::{
        Context as _, buffer,
        buffer::{Index as _, provided},
    },
};
use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

use crate::reactor::{
    buffers::{Cleanup, Create, Selected},
    mappings,
};

#[derive(Debug)]
pub struct Context<L: memory::raw::Receive> {
    buffers: mappings::Populated<u8>,
    layout: L,
}

impl<L: memory::raw::Receive> provided::Context for Context<L> {
    type Error = Create<L>;
    type Cleanup = Cleanup<L>;
    type Complete = Selected<L::Extent, <L::Extent as memory::raw::Extent>::Length>;

    fn new(layout: L) -> Result<Self, Self::Error> {
        let bytes = usize::from(layout.entries().get()) * layout.capacity().get() as usize;
        let mapping = mappings::Mapping::<u8>::allocate(bytes)?;
        let buffers = mapping.populate()?;
        Ok(Self { buffers, layout })
    }

    fn as_ptr(&self) -> *const u8 {
        self.buffers.as_ptr()
    }
}

impl<L: memory::raw::Receive> memory::Context for Context<L> {
    type Layout = L;

    fn layout(&self) -> L {
        self.layout
    }
}

impl<L: memory::raw::Receive> Close for Context<L> {
    type State = <mappings::Mapping<u8> as Close>::State;

    fn try_close(self) -> Result<(), Self::State> {
        self.buffers.try_close()
    }
}

impl<L: memory::raw::Receive>
    buffer::raw::Context<Selected<L::Extent, <L::Extent as memory::raw::Extent>::Length>>
    for Context<L>
{
    unsafe fn bytes<'a>(
        &'a self,
        buffer: &'a Selected<L::Extent, <L::Extent as memory::raw::Extent>::Length>,
    ) -> &'a [u8] {
        use buffer::raw::Token;
        use provided::Context as _;

        let pointer = self
            .as_ptr()
            .wrapping_add(buffer.index().offset(self.layout().capacity()).get() as usize);
        let len: bounded::U32<0, { MAX as u32 }> = buffer.length().into();
        // SAFETY: the caller retains this selected slot from this exact context;
        // completion admitted its live prefix against the original capacity.
        unsafe { slice::from_raw_parts(pointer, len.get() as usize) }
    }
}
