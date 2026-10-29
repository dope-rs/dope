use dope_sys::{memory, memory::buffer};

use crate::reactor::{buffers::Producer, cq::Buffer};

impl<L: memory::raw::Receive, T: buffer::raw::Token<Index = Buffer>> buffer::raw::Release<T>
    for Producer<L>
{
    unsafe fn release(&mut self, buffer: T) {
        self.defer(buffer);
    }
}
