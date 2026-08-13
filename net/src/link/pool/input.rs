use std::io;

use crate::wire::{self, receive};

pub struct Borrowed;
pub struct Retained;

pub trait Mode<W: wire::Wire> {
    #[doc(hidden)]
    type Kind;

    #[doc(hidden)]
    const DEFERS: bool;

    #[doc(hidden)]
    fn deferred_capacity(connections: usize, buffers: usize) -> io::Result<(usize, usize)> {
        if !Self::DEFERS {
            return Ok((0, 0));
        }
        let events = buffers.checked_add(connections).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: deferred receive capacity overflow",
            )
        })?;
        Ok((connections, events))
    }
}

impl<W: wire::Wire> Mode<W> for Borrowed {
    type Kind = Self;

    const DEFERS: bool = <W::Receive as receive::Strategy<W>>::BACKPRESSURE;
}

impl<W: wire::Wire> Mode<W> for Retained {
    type Kind = Self;

    const DEFERS: bool = W::RECV_CREDIT || <W::Receive as receive::Strategy<W>>::BACKPRESSURE;
}
