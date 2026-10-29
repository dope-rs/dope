use dope_sys::{engine::ring, memory::buffer};

use crate::{Engine, reactor::cq::Buffer};

impl<C: ring::Context, T: buffer::raw::Token<Index = Buffer>> buffer::raw::Release<T>
    for Engine<C>
{
    unsafe fn release(&mut self, buffer: T) {
        // SAFETY: this engine retains the original ring and the caller's unique slot.
        unsafe { buffer::raw::Release::release(&mut self.ring, buffer) };
    }
}
