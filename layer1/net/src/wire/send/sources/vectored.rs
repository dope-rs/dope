use dope_core::io::{socket::msg, transfer};
use o3::buffer::{segments::scatter::Source as _, storage::Zero};

use crate::wire::send::{self, Payload};

/// Retains the original message's lifetime, descriptor bound and exact byte sum.
pub struct Vectored<'a, const N: usize = { msg::MAX_IOVECS }> {
    message: msg::Vectored<'a, N>,
}

impl<'a, const N: usize> Vectored<'a, N> {
    pub(in crate::wire::send) fn chunks(&self) -> msg::raw::Cursor<'a> {
        self.message.chunks()
    }

    /// Wraps a message whose descriptors already retain their source owners.
    pub fn from_message(msg: msg::Vectored<'a, N>) -> Self {
        Self { message: msg }
    }

    #[must_use]
    /// Returns whether descriptors, including zero-length ones, contain no bytes.
    pub fn is_empty(&self) -> bool {
        self.message.is_empty()
    }

    #[must_use]
    /// Returns the exact payload length cached by the retained source.
    pub fn bytes(&self) -> transfer::Len {
        self.message.bytes()
    }
}

pub(in crate::wire::send) struct Vectors<'a> {
    pub(in crate::wire::send) message: msg::Message<'a>,
    pub(in crate::wire::send) bytes: transfer::NonZero,
}

impl<'scope, 'bytes: 'scope, const N: usize> Payload<'scope> for Vectored<'bytes, N> {
    fn select(&self) -> Option<impl send::raw::Ready<'scope>> {
        let message: msg::Message<'scope> = self.message.message();
        match transfer::NonZero::try_from(self.message.bytes()) {
            Ok(bytes) => Some(Vectors { message, bytes }),
            Err(Zero) => None,
        }
    }
}
