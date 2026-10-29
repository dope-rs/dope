use std::{marker, pin};

use dope_core::{
    dope_sys::io::socket::msg::{Header, Iovec},
    driver::route::{self, kind},
    io::{
        socket::msg,
        transfer::{self, progress},
    },
};
use o3::depot::{
    arena::pool::{Unique, recycle},
    array::copy,
};

use crate::{link::egress::metadata::lane, wire::send};

/// Pinned descriptor storage for one in-flight vectored send.
pub(in crate::link::egress) struct Entry<'d, const IOV: usize> {
    pub(super) iovecs: copy::Vec<Iovec, IOV>,
    header: Header,
    pub(super) bytes: progress::State,
    target: Option<route::Erased<'d, kind::Send>>,
    pin: marker::PhantomData<marker::PhantomPinned>,
}

impl<'d, const IOV: usize> Entry<'d, IOV> {
    pub(super) fn len(self: pin::Pin<&Self>) -> transfer::Len {
        self.bytes.len()
    }

    pub(super) fn matches(self: pin::Pin<&Self>, target: route::Erased<'d, kind::Send>) -> bool {
        self.target.is_some_and(|current| current.matches(target))
    }

    pub(super) fn mark(self: pin::Pin<&mut Self>, target: route::Erased<'d, kind::Send>) {
        // SAFETY: consuming Prepared::retain is the only caller, and recycle
        // clears the target before a new reservation can own this flight.
        let flight = unsafe { self.get_unchecked_mut() };
        flight.target = Some(target);
    }

    pub(super) fn vectored<'a>(self: pin::Pin<&'a mut Self>) -> send::Vectored<'a, IOV> {
        // SAFETY: the lane cannot acknowledge or clear entries before release.
        let flight = unsafe { self.get_unchecked_mut() };
        let iovecs = &flight.iovecs;
        // SAFETY: entry and descriptor owners remain retained through release.
        let bytes = flight.bytes.len();
        let msg = unsafe { msg::raw::Vectored::retain((&mut flight.header, iovecs, bytes)) };
        send::Vectored::from_message(msg)
    }
}

impl<const IOV: usize> Default for Entry<'_, IOV> {
    fn default() -> Self {
        Self {
            iovecs: copy::Vec::new(),
            header: Header::new(),
            bytes: progress::State::new(transfer::Len::MAX),
            target: None,
            pin: marker::PhantomData,
        }
    }
}

impl<const IOV: usize> Entry<'_, IOV> {
    fn reset(self: pin::Pin<&mut Self>) {
        // SAFETY: the lease reached terminal release; pinned fields do not move.
        let flight = unsafe { self.get_unchecked_mut() };
        flight.iovecs = copy::Vec::new();
        flight.bytes = progress::State::new(transfer::Len::MAX);
        flight.target = None;
    }
}

pub(in crate::link::egress) struct Lease<'p, 'd, const IOV: usize> {
    pub(super) buffer: Buffer<'p, 'd, IOV>,
    pub(super) prefix: lane::Prefix<'p, 'd>,
}

#[repr(transparent)]
pub(super) struct Buffer<'p, 'd, const IOV: usize>(
    pub(super) Unique<'p, Entry<'d, IOV>, recycle::Header<Entry<'d, IOV>>>,
);

impl<'p, 'd, const IOV: usize> Lease<'p, 'd, IOV> {
    pub(super) fn get(&self) -> pin::Pin<&Entry<'d, IOV>> {
        self.buffer.0.get()
    }

    pub(super) fn get_mut(&mut self) -> pin::Pin<&mut Entry<'d, IOV>> {
        self.buffer.0.get_mut()
    }

    pub(super) fn into_prefix(self) -> lane::Prefix<'p, 'd> {
        self.prefix
    }
}

impl<const IOV: usize> Drop for Buffer<'_, '_, IOV> {
    fn drop(&mut self) {
        self.0.get_mut().reset();
    }
}
