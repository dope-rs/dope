use std::mem;

pub use builder::Builder;
use dope_sys::io::{
    self, socket,
    socket::msg::{Header, Iovec},
};

use crate::io::transfer;

mod builder;
pub mod raw;

/// Maximum number of scatter/gather entries accepted by one socket message.
pub const MAX_IOVECS: usize = <crate::Backend as socket::Socket>::MAX_IOVECS;

/// A send message retaining at most `N` iovecs and their exact live byte sum.
pub struct Vectored<'a, const N: usize = MAX_IOVECS> {
    message: Message<'a>,
    iovs: &'a [Iovec],
    bytes: transfer::Len,
}

impl<'a, const N: usize> Vectored<'a, N> {
    #[must_use]
    pub fn message(&self) -> Message<'a> {
        self.message
    }

    #[must_use]
    pub const fn bytes(&self) -> transfer::Len {
        self.bytes
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes == transfer::Len::ZERO
    }
}

impl<'a, const N: usize> Vectored<'a, N> {
    fn from_parts(header: &'a mut Header, iovs: &'a [Iovec], bytes: transfer::Len) -> Self {
        *header = Header::new();
        header.bind(iovs);
        Self {
            message: Message { header },
            iovs,
            bytes,
        }
    }
}

/// A complete immutable send pointer graph.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Message<'a> {
    header: &'a Header,
}

// SAFETY: construction bounds the iovec count and byte sum, and Message retains
// the frozen header and its complete input graph through 'a.
unsafe impl<'a> io::raw::Message<'a> for Message<'a> {
    fn header(self) -> &'a Header {
        self.header
    }
}

const _: () = {
    assert!(mem::size_of::<Message<'static>>() == mem::size_of::<&Header>());
    assert!(mem::align_of::<Message<'static>>() == mem::align_of::<&Header>());
    assert!(mem::size_of::<Option<Message<'static>>>() == mem::size_of::<Message<'static>>());
};
