use std::marker;

use dope_sys::{
    memory,
    memory::{
        buffer,
        buffer::{Index as _, provided::Producer as _},
    },
};

use crate::reactor::{
    buffers::{Producer, Selected},
    cq::Buffer,
};

impl<L: memory::raw::Receive> buffer::raw::Select for Producer<L> {
    type Pending = Selected<L::Extent, <L::Extent as memory::raw::Extent>::Capacity>;
    type Index = Buffer;
    unsafe fn select(&self, buffer: Self::Index) -> Result<Self::Pending, Self::Index> {
        if buffer.is_within(self.layout().entries()) {
            return Ok(Selected {
                index: buffer,
                len: self.layout().limit(),
                extent: marker::PhantomData,
            });
        }
        Err(buffer)
    }
}
