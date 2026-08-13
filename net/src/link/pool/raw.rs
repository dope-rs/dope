use std::io;

use dope_core::{
    driver::{self, lifecycle::routing, ops, route},
    io::fd::handles,
};
use o3::buffer::storage;

use crate::{
    link::pool::{self, outbound, pending},
    wire,
};

#[doc(hidden)]
pub trait Bind<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize> {
    /// # Safety
    /// The owner must install, shut down, and finish the returned pool.
    unsafe fn bind(self, route: routing::Route<'d, ID>) -> pool::Pool<'d, ID, T, W, S, M, B, IOV>;
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Bind<'d, ID, T, W, S, M, B, IOV> for pool::Prepared<'d, ID, T, W, S, M, B, IOV>
{
    unsafe fn bind(self, route: routing::Route<'d, ID>) -> pool::Pool<'d, ID, T, W, S, M, B, IOV> {
        let keys = pool::Keyspace::from_route(&route);
        pool::Pool {
            storage: pool::Connections {
                route,
                keys,
                prepared: self,
            },
        }
    }
}

/// Route-branded outbound slots derived from fully allocated pool storage.
///
/// Construction is fallible; binding a route is a linear, infallible move.
#[doc(hidden)]
pub struct PreparedOutbound<
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B = storage::Shared,
    const IOV: usize = 32,
> {
    prepared: pool::Prepared<'d, ID, T, W, S, M, B, IOV>,
    reservation: Reservation<'d, ID>,
    addresses: outbound::AddressTable<'d, ID>,
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    PreparedOutbound<'d, ID, T, W, S, M, B, IOV>
{
    pub fn reserve(
        prepared: pool::Prepared<'d, ID, T, W, S, M, B, IOV>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        let capacity = prepared.slab.capacity().raw();
        let addresses = outbound::AddressTable::try_with_capacity(capacity)?;
        let reservation = ops::Files::reserve_outbound(driver, capacity)?;
        Ok(Self {
            prepared,
            reservation: Reservation::new(reservation),
            addresses,
        })
    }

    /// # Safety
    /// The owner must install, shut down, and finish the returned pool.
    pub unsafe fn bind(
        self,
        route: routing::Route<'d, ID>,
    ) -> pool::Outbound<'d, ID, T, W, S, M, B, IOV> {
        let pool = unsafe { Bind::bind(self.prepared, route) };
        pool::Outbound {
            storage: pool.storage,
            outbound: self.reservation,
            addresses: self.addresses,
        }
    }
}

pub(super) struct Reservation<'d, const ID: u8> {
    reservation: ops::OutboundReservation<'d, ID>,
}

impl<'d, const ID: u8> Reservation<'d, ID> {
    fn new(reservation: ops::OutboundReservation<'d, ID>) -> Self {
        Self { reservation }
    }

    pub(super) fn descriptor<U>(
        &self,
        vacancy: &pending::Vacancy<'_, 'd, ID, U>,
    ) -> Option<route::Bound<'d, route::KeyTag<ID>, handles::SocketSlot<'d>>> {
        let target = vacancy.key().target();
        self.reservation.bind(target)
    }
}
