use std::{pin, ptr};

use dope_core::{
    driver::route,
    io::{socket::msg, transfer},
};
use o3::collections::fixed::pinned::recycle;

use crate::{link::egress::queue::flight, wire::send};

// SAFETY: this private source is constructed only from the flight pool inside
// egress::Storage. Committed leases move into connection lanes, which release
// them only after terminal completion or quiescence. Prepared declares its
// connection slab before egress storage, so every lane drops before this pool.
unsafe impl<'d, const IOV: usize> recycle::raw::PoolOwner<'d, flight::Flight<IOV>>
    for flight::Source<'d, IOV>
{
    fn pool(self) -> ptr::NonNull<recycle::Pool<flight::Flight<IOV>>> {
        self.pool
    }
}

pub(in crate::link::egress::queue) trait Access<'a, const IOV: usize> {
    fn reset(self);
    fn push(self, iovec: msg::Iovec) -> bool;
    fn mark(self, target: route::Token);
    fn vectored(self) -> send::Vectored<'a>;
}

impl<'a, const IOV: usize> Access<'a, IOV> for pin::Pin<&'a mut flight::Flight<IOV>> {
    fn reset(self) {
        // SAFETY: the lease reached terminal release; pinned fields do not move.
        let flight = unsafe { self.get_unchecked_mut() };
        flight.len = 0;
        flight.bytes = transfer::Len::ZERO;
        flight.target = None;
    }

    fn push(self, iovec: msg::Iovec) -> bool {
        // SAFETY: only scalar prefix state changes before submission.
        let flight = unsafe { self.get_unchecked_mut() };
        let Some(bytes) = flight.bytes.checked_add(iovec.len()) else {
            return false;
        };
        let Some(slot) = flight.iovecs.get_mut(flight.len) else {
            return false;
        };
        flight.bytes = bytes;
        *slot = iovec;
        flight.len += 1;
        true
    }

    fn mark(self, target: route::Token) {
        // SAFETY: Prepared owns exclusive access before committing the lease.
        let flight = unsafe { self.get_unchecked_mut() };
        debug_assert!(flight.target.is_none());
        flight.target = Some(target);
    }

    fn vectored(self) -> send::Vectored<'a> {
        // SAFETY: the lane cannot acknowledge or clear entries before release.
        let flight = unsafe { self.get_unchecked_mut() };
        let iovecs = &flight.iovecs[..flight.len];
        // SAFETY: entry and descriptor owners remain retained through release.
        let message =
            unsafe { msg::raw::Vectored::retain((&mut flight.header, iovecs, flight.bytes)) };
        send::Vectored::from_message(message)
    }
}
