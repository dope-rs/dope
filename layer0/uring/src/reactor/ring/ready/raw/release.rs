use dope_sys::{memory, memory::buffer};

use crate::reactor::{cq, ring::Ready};

impl<T, L: memory::raw::Receive, P: buffer::raw::Token<Index = cq::Buffer>> buffer::raw::Release<P>
    for Ready<T, L>
{
    unsafe fn release(&mut self, buffer: P) {
        // SAFETY: the caller returns this ring's unique slot to its retained producer.
        unsafe { buffer::raw::Release::release(self.owner.storage_mut(), buffer) };
    }
}
