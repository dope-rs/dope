use dope_core::driver::{self, route::table::conn, settings};
use o3::{
    depot,
    depot::{index::Initialized, queue::pool},
};

use crate::{
    link::{
        event,
        pool::recv::storage::{self, Extent, queue},
    },
    wire::receive::mode::{Disabled, Enabled, Parking, Waiting},
};

/// # Safety
/// Extent, observation, and projection describe the same fixed storage domain.
/// Empty storage projects every admitted logical table slot to its initialized unit.
pub unsafe trait Mode {
    type Storage<'d>;
    type Bound<'a, 'd: 'a>: queue::Ops<'d>;

    /// Allocates this mode's retained storage for one admitted pool.
    fn try_storage<'d>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        driver: driver::Reference<'d>,
    ) -> crate::Result<Self::Storage<'d>>;

    fn extent<'d>(storage: &Self::Storage<'d>) -> Extent;

    /// Consumes an admitted index from this storage's original paired table.
    fn has_queued<'d>(
        slot: depot::raw::Proof<(&Self::Storage<'d>, usize), Initialized<()>>,
    ) -> bool;

    /// Retains the original admitted slot's exclusive storage borrow.
    fn bind<'a, 'd: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd>;
}

// SAFETY: each admitted logical table slot projects to the initialized unit.
// The complete exclusive storage loan prevents competing mutable projections.
unsafe impl storage::raw::Mode for Disabled {
    type Storage<'d> = ();
    type Bound<'a, 'd: 'a> = &'a mut ();

    fn try_storage<'d>(
        _: conn::Capacity,
        _: settings::Buffers,
        _: driver::Reference<'d>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn extent<'d>((): &()) -> Extent {
        Extent::Empty
    }

    fn has_queued<'d>(_: depot::raw::Proof<(&(), usize), Initialized<()>>) -> bool {
        false
    }

    fn bind<'a, 'd: 'a>(
        slot: depot::raw::Proof<(&'a mut (), usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd> {
        slot.into_inner().0
    }
}

// SAFETY: allocation, observation, and binding retain the same fixed event lanes.
unsafe impl storage::raw::Mode for Parking {
    type Storage<'d> = storage::Parking<'d>;
    type Bound<'a, 'd: 'a> = pool::Lane<'a, event::Completion<'d>>;

    fn try_storage<'d>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        _: driver::Reference<'d>,
    ) -> crate::Result<storage::Parking<'d>> {
        storage::Parking::try_new(cap, bufs)
    }

    fn extent<'d>(storage: &Self::Storage<'d>) -> Extent {
        Extent::Lanes(storage.events.len())
    }

    fn has_queued<'d>(
        slot: depot::raw::Proof<(&Self::Storage<'d>, usize), Initialized<()>>,
    ) -> bool {
        let (storage, index) = slot.into_inner();
        // SAFETY: the caller carries admission to this exact paired event lane.
        unsafe { has_queued(&storage.events, index) }
    }

    fn bind<'a, 'd: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd> {
        let (storage, index) = slot.into_inner();
        use o3::depot::raw::Proof;
        // SAFETY: the paired table proved this index in the unchanged event extent.
        unsafe { Proof::new((&mut storage.events, index)) }.into()
    }
}

// SAFETY: the admitted event and credit extents share one original connection table.
unsafe impl storage::raw::Mode for Enabled {
    type Storage<'d> = storage::Retained<'d>;
    type Bound<'a, 'd: 'a> = storage::Loan<'a, 'd>;

    fn try_storage<'d>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        driver: driver::Reference<'d>,
    ) -> crate::Result<storage::Retained<'d>> {
        retained(cap, bufs, driver)
    }

    fn extent<'d>(storage: &Self::Storage<'d>) -> Extent {
        Extent::Lanes(storage.events.events.len())
    }

    fn has_queued<'d>(
        slot: depot::raw::Proof<(&Self::Storage<'d>, usize), Initialized<()>>,
    ) -> bool {
        let (storage, index) = slot.into_inner();
        // SAFETY: admission pairs this table index with the retained event extent.
        unsafe { has_queued(&storage.events.events, index) }
    }

    fn bind<'a, 'd: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd> {
        let (storage, index) = slot.into_inner();
        // SAFETY: this mode retains the exact admitted event and credit owners.
        unsafe { bind_retained(storage, index) }
    }
}

// SAFETY: waiting changes reservation policy, preserving enabled ingress storage geometry.
unsafe impl<R> storage::raw::Mode for Waiting<R> {
    type Storage<'d> = storage::Retained<'d>;
    type Bound<'a, 'd: 'a> = storage::Loan<'a, 'd>;

    fn try_storage<'d>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        driver: driver::Reference<'d>,
    ) -> crate::Result<storage::Retained<'d>> {
        <Enabled as storage::raw::Mode>::try_storage(cap, bufs, driver)
    }

    fn extent<'d>(storage: &Self::Storage<'d>) -> Extent {
        <Enabled as storage::raw::Mode>::extent(storage)
    }

    fn has_queued<'d>(
        slot: depot::raw::Proof<(&Self::Storage<'d>, usize), Initialized<()>>,
    ) -> bool {
        <Enabled as storage::raw::Mode>::has_queued(slot)
    }

    fn bind<'a, 'd: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd> {
        <Enabled as storage::raw::Mode>::bind(slot)
    }
}

/// Retains driver-backed credit and buffers for one admitted pool.
fn retained<'d>(
    cap: conn::Capacity,
    bufs: settings::Buffers,
    driver: driver::Reference<'d>,
) -> crate::Result<storage::Retained<'d>> {
    let credit = driver
        .scheduler()
        .try_storage(cap)
        .map_err(crate::Resource::Credit)?;
    storage::Retained::try_new(cap, bufs, credit)
}

/// # Safety
/// The index belongs to the original table paired with these event lanes.
unsafe fn has_queued(events: &pool::Lanes<event::Completion<'_>>, index: usize) -> bool {
    use o3::depot::raw::Proof;
    // SAFETY: admission supplies a valid index and this shared borrow retains its owner.
    !unsafe { Proof::new((events, index)) }.into().is_empty()
}

/// # Safety
/// The index belongs to the original table paired with this retained storage.
unsafe fn bind_retained<'a, 'd: 'a>(
    storage: &'a mut storage::Retained<'d>,
    index: usize,
) -> storage::Loan<'a, 'd> {
    use o3::depot::raw::Proof;
    // SAFETY: admission supplies the exact lane; the returned loan retains its allocation.
    let events = unsafe { Proof::new((&mut storage.events.events, index)) }.into();
    // SAFETY: retained constructs credit and event storage from the same logical
    // capacity; this original owner and its active generation remain borrowed.
    let credit = unsafe { Proof::new((&storage.credit, index)) }.into();
    storage::Loan::new(events, credit)
}
