use std::marker;

use dope_sys::{memory, memory::buffer};

use crate::reactor::cq::Buffer;

#[derive(Debug)]
pub struct Selected<E: memory::raw::Extent, L> {
    pub(super) index: Buffer,
    pub(super) len: L,
    pub(super) extent: marker::PhantomData<fn(E) -> E>,
}

impl<E: memory::raw::Extent, L> buffer::Recycle for Selected<E, L> {
    type Deferred = Self;

    fn recycle(self) -> Option<Self> {
        Some(self)
    }
}

// SAFETY: construction requires unique slot ownership; completion consumes it.
unsafe impl<E: memory::raw::Extent, L: Copy> buffer::raw::Token for Selected<E, L> {
    type Index = Buffer;
    type Length = L;

    fn index(&self) -> Self::Index {
        self.index
    }

    fn length(&self) -> Self::Length {
        self.len
    }
}
