use std::mem;

use dope_core::io::socket::msg;

mod sealed;

pub(in crate::link::egress) use sealed::Prepare;

#[repr(transparent)]
pub(in crate::link::egress) struct Entry<B>(B);

const _: () = assert!(mem::size_of::<Entry<&'static [u8]>>() == 2 * mem::size_of::<usize>());

pub(in crate::link::egress) struct Part {
    pub(in crate::link::egress) iovec: msg::Iovec,
    pub(in crate::link::egress) available: usize,
}

impl<B> Entry<B> {
    pub(in crate::link::egress) fn retained(value: B) -> Self {
        Self(value)
    }
}
