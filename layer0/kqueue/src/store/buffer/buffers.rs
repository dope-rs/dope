use std::{convert::Infallible, mem};

use dope_sys::memory::buffer::Recycle;
use o3::buffer::{
    self,
    pool::{self, owners, state},
};

/// Initialized storage issued only by an ABI-bounded receive pool.
#[repr(transparent)]
pub struct Buffer(
    pub(in crate::store::buffer) pool::Span<owners::Plain, pool::Owned, state::Initialized>,
);

impl dope_sys::Buffer for Buffer {
    fn output(&mut self) -> buffer::raw::Output<'_> {
        self.0.output()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn recycle(self) {
        self.0.recycle();
    }
}

impl Recycle for Buffer {
    type Deferred = Infallible;

    fn recycle(self) -> Option<Self::Deferred> {
        self.0.recycle();
        None
    }
}

const _: () = {
    assert!(
        mem::size_of::<Buffer>()
            == mem::size_of::<pool::Span<owners::Plain, pool::Owned, state::Initialized>>()
    );
    assert!(
        mem::align_of::<Buffer>()
            == mem::align_of::<pool::Span<owners::Plain, pool::Owned, state::Initialized>>()
    );
    assert!(
        mem::size_of::<Option<Buffer>>()
            == mem::size_of::<Option<pool::Span<owners::Plain, pool::Owned, state::Initialized>>>()
    );
};
