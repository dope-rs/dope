use std::num;

use dope_sys::memory;
use o3::buffer;

use crate::store::buffer::Buffer;

#[repr(transparent)]
pub struct Context<L>(pub(super) L);

impl<L: memory::raw::Layout> memory::Context for Context<L> {
    type Layout = L;

    fn layout(&self) -> L {
        self.0
    }
}

impl<L: memory::raw::Layout> buffer::raw::Fixed<Buffer> for Context<L> {
    fn resident(&self) -> num::NonZero<u32> {
        self.0.capacity()
    }

    unsafe fn project<'a, 'id>(
        &'a self,
        buffer: &'a mut Buffer,
        limit: buffer::raw::Limit<'id>,
    ) -> buffer::raw::Projection<'a, 'id> {
        // SAFETY: the caller retains this buffer's original pool context and
        // matching arena ceiling; its lease preserves the initialized prefix.
        unsafe { limit.project(buffer.0.as_mut_slice()) }
    }
}
