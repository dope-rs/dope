use std::{marker, pin};

use dope_core::{
    dope_sys::io::socket::msg::{Header, Iovec},
    io::{socket::msg, transfer},
};
use o3::depot::{arena::pool::Unique, array::copy};

use crate::{link::egress::metadata::lane, wire::send};

/// Pinned descriptor storage for one in-flight vectored send.
pub(in crate::link::egress) struct Entry<const IOV: usize> {
    pub(super) iovecs: copy::Vec<Iovec, IOV>,
    header: Header,
    pub(super) bytes: transfer::Len,
    pin: marker::PhantomData<marker::PhantomPinned>,
}

impl<const IOV: usize> Entry<IOV> {
    pub(super) fn len(self: pin::Pin<&Self>) -> transfer::Len {
        self.bytes
    }

    pub(super) fn vectored<'a>(self: pin::Pin<&'a mut Self>) -> send::Vectored<'a, IOV> {
        // SAFETY: the lane cannot acknowledge or clear entries before release.
        let flight = unsafe { self.get_unchecked_mut() };
        let iovecs = &flight.iovecs;
        // SAFETY: entry and descriptor owners remain retained through release.
        let msg = unsafe { msg::raw::Vectored::retain((&mut flight.header, iovecs, flight.bytes)) };
        send::Vectored::from_message(msg)
    }
}

impl<const IOV: usize> Default for Entry<IOV> {
    fn default() -> Self {
        Self {
            iovecs: copy::Vec::new(),
            header: Header::new(),
            bytes: transfer::Len::ZERO,
            pin: marker::PhantomData,
        }
    }
}

impl<const IOV: usize> Entry<IOV> {
    fn reset(self: pin::Pin<&mut Self>) {
        // SAFETY: the lease reached terminal release; pinned fields do not move.
        let flight = unsafe { self.get_unchecked_mut() };
        flight.iovecs = copy::Vec::new();
        flight.bytes = transfer::Len::ZERO;
    }
}

pub(in crate::link::egress) struct Lease<'p, 'd, const IOV: usize> {
    pub(super) buffer: Buffer<'p, IOV>,
    pub(super) prefix: lane::Prefix<'p, 'd>,
}

#[repr(transparent)]
pub(super) struct Buffer<'p, const IOV: usize>(pub(super) Unique<'p, Entry<IOV>>);

impl<'p, 'd, const IOV: usize> Lease<'p, 'd, IOV> {
    pub(super) fn get(&self) -> pin::Pin<&Entry<IOV>> {
        self.buffer.0.get()
    }

    pub(super) fn get_mut(&mut self) -> pin::Pin<&mut Entry<IOV>> {
        self.buffer.0.get_mut()
    }

    pub(super) fn into_prefix(self) -> lane::Prefix<'p, 'd> {
        self.prefix
    }
}

impl<const IOV: usize> Drop for Buffer<'_, IOV> {
    fn drop(&mut self) {
        self.0.get_mut().reset();
    }
}
