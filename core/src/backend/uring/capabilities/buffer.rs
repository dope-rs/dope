use crate::{
    backend::{self, uring},
    platform,
};

impl platform::Buffer for backend::Uring {
    type Token = uring::ffi::Buffer;

    fn release(&mut self, buffer: Self::Token) {
        self.ring.buffers().provided().defer(buffer);
    }
}
