use std::{num, slice};

use dope_sys::{
    memory,
    memory::{
        Context as _,
        buffer::{Index as _, provided::Context as _},
    },
};
use o3::{base::num::bounded, buffer, buffer::bytes::bounded::MAX};

use crate::reactor::buffers::{self, Selected};

impl<L: memory::raw::Receive>
    buffer::raw::Fixed<Selected<L::Extent, <L::Extent as memory::raw::Extent>::Length>>
    for buffers::raw::Context<L>
{
    fn resident(&self) -> num::NonZero<u32> {
        self.layout().capacity()
    }

    unsafe fn project<'a, 'id>(
        &'a self,
        buffer: &'a mut Selected<L::Extent, <L::Extent as memory::raw::Extent>::Length>,
        limit: buffer::raw::Limit<'id>,
    ) -> buffer::raw::Projection<'a, 'id> {
        use dope_sys::memory::buffer::raw::Token;

        let len: bounded::U32<0, { MAX as u32 }> = buffer.length().into();
        let pointer = self
            .as_ptr()
            .wrapping_add(buffer.index().offset(self.layout().capacity()).get() as usize);
        // SAFETY: the caller binds this selected owner and the arena's resident
        // ceiling to this exact context and its original immutable layout.
        unsafe {
            limit.project(slice::from_raw_parts_mut(
                pointer.cast_mut(),
                len.get() as usize,
            ))
        }
    }
}
