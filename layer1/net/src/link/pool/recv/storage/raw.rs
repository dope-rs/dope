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
    type Storage<'d, R>;
    type Bound<'a, 'd: 'a, R: 'a>: queue::Ops<'d, R>;

    /// Allocates this mode's retained storage for one admitted pool.
    fn try_storage<'d, R>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        driver: driver::Reference<'d>,
    ) -> crate::Result<Self::Storage<'d, R>>;

    fn extent<'d, R>(storage: &Self::Storage<'d, R>) -> Extent;

    /// Consumes an admitted index from this storage's original paired table.
    fn has_queued<'d, R>(
        slot: depot::raw::Proof<(&Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> bool;

    /// Retains the original admitted slot's exclusive storage borrow.
    fn bind<'a, 'd: 'a, R: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd, R>;
}

// SAFETY: each admitted logical table slot projects to the initialized unit.
// The complete exclusive storage loan prevents competing mutable projections.
unsafe impl storage::raw::Mode for Disabled {
    type Storage<'d, R> = ();
    type Bound<'a, 'd: 'a, R: 'a> = &'a mut ();

    fn try_storage<'d, R>(
        _: conn::Capacity,
        _: settings::Buffers,
        _: driver::Reference<'d>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn extent<'d, R>((): &()) -> Extent {
        Extent::Empty
    }

    fn has_queued<'d, R>(_: depot::raw::Proof<(&(), usize), Initialized<()>>) -> bool {
        false
    }

    fn bind<'a, 'd: 'a, R: 'a>(
        slot: depot::raw::Proof<(&'a mut (), usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd, R> {
        slot.into_inner().0
    }
}

// SAFETY: allocation, observation, and binding retain the same fixed event lanes.
unsafe impl storage::raw::Mode for Parking {
    type Storage<'d, R> = storage::Parking<'d, R>;
    type Bound<'a, 'd: 'a, R: 'a> = pool::Lane<'a, event::Completion<'d, R>>;

    fn try_storage<'d, R>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        _: driver::Reference<'d>,
    ) -> crate::Result<storage::Parking<'d, R>> {
        storage::Parking::try_new(cap, bufs)
    }

    fn extent<'d, R>(storage: &Self::Storage<'d, R>) -> Extent {
        Extent::Lanes(storage.events.len())
    }

    fn has_queued<'d, R>(
        slot: depot::raw::Proof<(&Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> bool {
        let (storage, index) = slot.into_inner();
        // SAFETY: the caller carries admission to this exact paired event lane.
        unsafe { has_queued(&storage.events, index) }
    }

    fn bind<'a, 'd: 'a, R: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd, R> {
        let (storage, index) = slot.into_inner();
        use o3::depot::raw::Proof;
        // SAFETY: the paired table proved this index in the unchanged event extent.
        unsafe { Proof::new((&mut storage.events, index)) }.into()
    }
}

// SAFETY: the admitted event and credit extents share one original connection table.
unsafe impl storage::raw::Mode for Enabled {
    type Storage<'d, R> = storage::Retained<'d, R>;
    type Bound<'a, 'd: 'a, R: 'a> = storage::Loan<'a, 'd, R>;

    fn try_storage<'d, R>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        driver: driver::Reference<'d>,
    ) -> crate::Result<storage::Retained<'d, R>> {
        retained(cap, bufs, driver)
    }

    fn extent<'d, R>(storage: &Self::Storage<'d, R>) -> Extent {
        Extent::Lanes(storage.events.events.len())
    }

    fn has_queued<'d, R>(
        slot: depot::raw::Proof<(&Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> bool {
        let (storage, index) = slot.into_inner();
        // SAFETY: admission pairs this table index with the retained event extent.
        unsafe { has_queued(&storage.events.events, index) }
    }

    fn bind<'a, 'd: 'a, R: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd, R> {
        let (storage, index) = slot.into_inner();
        // SAFETY: this mode retains the exact admitted event and credit owners.
        unsafe { bind_retained(storage, index) }
    }
}

// SAFETY: waiting changes reservation policy, preserving enabled ingress storage geometry.
unsafe impl<Q> storage::raw::Mode for Waiting<Q> {
    type Storage<'d, R> = storage::Retained<'d, R>;
    type Bound<'a, 'd: 'a, R: 'a> = storage::Loan<'a, 'd, R>;

    fn try_storage<'d, R>(
        cap: conn::Capacity,
        bufs: settings::Buffers,
        driver: driver::Reference<'d>,
    ) -> crate::Result<storage::Retained<'d, R>> {
        <Enabled as storage::raw::Mode>::try_storage(cap, bufs, driver)
    }

    fn extent<'d, R>(storage: &Self::Storage<'d, R>) -> Extent {
        <Enabled as storage::raw::Mode>::extent(storage)
    }

    fn has_queued<'d, R>(
        slot: depot::raw::Proof<(&Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> bool {
        <Enabled as storage::raw::Mode>::has_queued(slot)
    }

    fn bind<'a, 'd: 'a, R: 'a>(
        slot: depot::raw::Proof<(&'a mut Self::Storage<'d, R>, usize), Initialized<()>>,
    ) -> Self::Bound<'a, 'd, R> {
        <Enabled as storage::raw::Mode>::bind(slot)
    }
}

/// Retains driver-backed credit and buffers for one admitted pool.
fn retained<'d, R>(
    cap: conn::Capacity,
    bufs: settings::Buffers,
    driver: driver::Reference<'d>,
) -> crate::Result<storage::Retained<'d, R>> {
    let credit = driver
        .scheduler()
        .try_storage(cap)
        .map_err(crate::Resource::Credit)?;
    storage::Retained::try_new(cap, bufs, credit)
}

/// # Safety
/// The index belongs to the original table paired with these event lanes.
unsafe fn has_queued<R>(events: &pool::Lanes<event::Completion<'_, R>>, index: usize) -> bool {
    use o3::depot::raw::Proof;
    // SAFETY: admission supplies a valid index and this shared borrow retains its owner.
    !unsafe { Proof::new((events, index)) }.into().is_empty()
}

/// # Safety
/// The index belongs to the original table paired with this retained storage.
unsafe fn bind_retained<'a, 'd: 'a, R>(
    storage: &'a mut storage::Retained<'d, R>,
    index: usize,
) -> storage::Loan<'a, 'd, R> {
    use o3::depot::raw::Proof;
    // SAFETY: admission supplies the exact lane; the returned loan retains its allocation.
    let events = unsafe { Proof::new((&mut storage.events.events, index)) }.into();
    // SAFETY: retained constructs credit and event storage from the same logical
    // capacity; this original owner and its active generation remain borrowed.
    let credit = unsafe { Proof::new((&storage.credit, index)) }.into();
    storage::Loan::new(events, credit)
}
