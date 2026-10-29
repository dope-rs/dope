use dope_core::{
    driver::{
        self,
        retained::flight,
        route::{Identity, lease, table},
        storage,
    },
    io::fd::outbound::Reserved,
};
use o3::buffer::storage::Shared;

use crate::{
    link::{
        egress::reserve::demand,
        pool::{self, change, outbound, recv::input},
    },
    wire,
};

/// Route-branded outbound slots derived from fully allocated pool storage.
///
/// Construction is fallible; binding a route is a linear, infallible move.
pub struct Prepared<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B = Shared,
    const IOV: usize = 32,
    D: demand::Mode = demand::Fallible,
    F = flight::Need<ID>,
> where
    'd: 'w,
{
    prepared: change::raw::Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D, ()>,
    flights: F,
    reservation: Reserved<'d, ID>,
    addresses: outbound::AddressTable<'d, ID, T::Prepared>,
}

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    'd: 'w,
{
    pub fn try_reserve(
        (base, flights): change::raw::Admission<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        files: table::Capacity,
        driver: &driver::Context<'_, 'd>,
    ) -> crate::Result<Self> {
        let slots = base.slab.view().capacity();
        if files.count().get() < slots.count().get() {
            return Err(crate::Resource::Config(
                crate::ConfigError::PhysicalBelowLogical {
                    logical: slots,
                    physical: files,
                },
            ));
        }
        let addrs = outbound::AddressTable::try_new(slots).map_err(crate::Resource::Allocation)?;
        let lease = Reserved::<ID>::try_new(driver, files).map_err(crate::Resource::Descriptor)?;
        Ok(Self {
            prepared: base,
            flights,
            reservation: lease,
            addresses: addrs,
        })
    }

    pub fn try_flights(
        self,
        store: storage::Context<'_, 'd>,
    ) -> crate::Result<Admitted<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>> {
        let Self {
            prepared,
            flights,
            reservation: lease,
            addresses: addrs,
        } = self;
        let flights = store
            .try_flights(flights)
            .map_err(crate::Resource::Allocation)?
            .slots();
        Ok(Prepared {
            prepared,
            flights,
            reservation: lease,
            addresses: addrs,
        })
    }
}

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Admitted<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    'd: 'w,
{
    /// # Safety
    /// The owner must install, shut down, and finish the returned pool.
    pub unsafe fn bind(
        self,
        route: lease::Claim<'d, ID>,
    ) -> pool::Outbound<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        let Self {
            prepared,
            flights,
            reservation: lease,
            addresses: addrs,
        } = self;
        let pool = unsafe { prepared.commit(flights).bind(route) };
        pool::Outbound {
            storage: pool.storage,
            outbound: lease,
            addresses: addrs,
        }
    }
}

type Admitted<'p, 'w, 'd, ID, T, W, S, M, B, const IOV: usize, D> =
    Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D, flight::Slots<'d, ID>>;
