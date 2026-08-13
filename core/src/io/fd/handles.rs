use std::{fmt, mem, ops, process};

use o3::{collections::batch::set, permit};

use crate::{
    backend::{self, fixed},
    driver::{self, flight, route, schedule::ready},
    io,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct FixedSlot(route::SlotIndex, o3::ThreadBound);

const _: () = assert!(mem::size_of::<FixedSlot>() == mem::size_of::<u32>());

#[must_use = "accepted fixed-file authority must be delivered or reclaimed"]
#[repr(transparent)]
pub(crate) struct Accepted(FixedSlot);

#[must_use = "created fixed-file authority must be delivered or reclaimed"]
#[repr(transparent)]
pub(crate) struct Created(FixedSlot);

const _: () = assert!(mem::size_of::<Accepted>() == mem::size_of::<FixedSlot>());

impl FixedSlot {
    pub(crate) const fn from_index(index: route::SlotIndex) -> Self {
        use o3::ThreadBound;
        Self(index, ThreadBound::NEW)
    }

    pub(crate) fn raw(self) -> u32 {
        self.0.raw()
    }

    pub(crate) fn token_index(self) -> route::SlotIndex {
        self.0
    }
}

impl set::DenseIndex for FixedSlot {
    fn into_usize(self) -> usize {
        self.raw() as usize
    }

    fn from_usize(raw: usize) -> Self {
        Self::from_index(route::SlotIndex::from_bounded(raw as u32))
    }
}

impl Accepted {
    pub(crate) const fn from_live(slot: FixedSlot) -> Self {
        Self(slot)
    }

    pub(crate) fn bind<'d>(self, driver: driver::Reference<'d>) -> Option<AcceptedSlot<'d>> {
        let slot = self.0;
        let Some(lease) = FixedLease::claim(slot, driver) else {
            driver.maintenance().defer_descriptor(slot, None);
            return None;
        };
        Some(AcceptedSlot { lease })
    }

    pub(crate) fn into_slot(self) -> FixedSlot {
        self.0
    }
}

impl Created {
    pub(crate) const fn from_live(slot: FixedSlot) -> Self {
        Self(slot)
    }

    pub(crate) fn bind<'d>(self, driver: driver::Reference<'d>) -> Option<CreatedSlot<'d>> {
        match driver.outbound().complete_outbound_create_success(self.0) {
            driver::CreateSuccess::Deliver(outbound) => Some(CreatedSlot {
                driver,
                slot: self.0,
                outbound,
            }),
            driver::CreateSuccess::Close(close) => {
                driver.maintenance().defer_close(close);
                None
            }
        }
    }
}

#[repr(transparent)]
pub(crate) struct FixedLease<'d> {
    lease: permit::Lease<FixedReturn<'d>>,
}

struct FixedReturn<'d> {
    driver: driver::Reference<'d>,
}

struct FixedOwned {
    slot: FixedSlot,
    epoch: u32,
}

impl<'d> FixedReturn<'d> {
    fn key(&self, owned: &FixedOwned) -> ready::FixedKey<'d> {
        ready::FixedKey::new(owned.slot.raw(), owned.epoch)
    }

    fn release(&self, owned: FixedOwned) -> Option<ready::FixedRelease<'d>> {
        self.driver.ready().release_fixed_ready(self.key(&owned))
    }
}

impl permit::Return for FixedReturn<'_> {
    type Item = FixedOwned;

    fn return_item(&self, owned: Self::Item) {
        if let Some(released) = self.release(owned) {
            self.driver.maintenance().retire_fixed_release(released);
        }
    }
}

impl<'d> FixedLease<'d> {
    fn new(driver: driver::Reference<'d>, slot: FixedSlot, epoch: u32) -> Self {
        Self {
            lease: permit::Lease::new(FixedReturn { driver }, FixedOwned { slot, epoch }),
        }
    }

    fn claim(slot: FixedSlot, driver: driver::Reference<'d>) -> Option<Self> {
        let key = driver.ready().claim_fixed_ready(slot)?;
        Some(Self::new(driver, slot, key.epoch()))
    }

    fn from_reserved(slot: fixed::Slot<'d>, driver: driver::Reference<'d>) -> Option<Self> {
        let fixed = slot.fixed();
        let Some(key) = driver.ready().claim_fixed_ready(fixed) else {
            driver.maintenance().defer_fixed_slot(slot.retire());
            return None;
        };
        Some(Self::new(driver, slot.into_claimed(key), key.epoch()))
    }

    pub(crate) fn slot(&self) -> FixedSlot {
        self.lease.item().slot
    }

    fn slot_ref(&self) -> &FixedSlot {
        &self.lease.item().slot
    }

    fn key(&self) -> ready::FixedKey<'d> {
        self.lease.sink().key(self.lease.item())
    }

    pub(super) fn driver(&self) -> driver::Reference<'d> {
        self.lease.sink().driver
    }

    fn ready_handle(&self) -> ready::Handle<'d> {
        self.driver().ready().fixed_ready(self.key())
    }

    fn release(self) -> ready::FixedRelease<'d> {
        let (return_fixed, owned) = self.lease.into_parts();
        let Some(released) = return_fixed.release(owned) else {
            process::abort();
        };
        released
    }
}

const _: () = {
    assert!(
        mem::size_of::<FixedLease<'static>>()
            == mem::size_of::<(driver::Reference<'static>, FixedSlot, u32)>()
    );
    assert!(
        mem::align_of::<FixedLease<'static>>()
            == mem::align_of::<(driver::Reference<'static>, FixedSlot, u32)>()
    );
};

#[repr(C)]
pub struct AcceptedSlot<'d> {
    lease: FixedLease<'d>,
}

#[must_use = "created socket must be activated or reclaimed"]
#[repr(C)]
/// The successful kernel half of one driver-scoped socket creation.
///
/// Its invariant driver lifetime cannot be shortened into another scope.
///
/// ```compile_fail
/// use dope_core::io::fd::handles::CreatedSlot;
///
/// fn shorten<'long: 'short, 'short>(created: CreatedSlot<'long>) -> CreatedSlot<'short> {
///     created
/// }
/// ```
pub struct CreatedSlot<'d> {
    driver: driver::Reference<'d>,
    slot: FixedSlot,
    outbound: driver::OutboundKey,
}

const _: () =
    assert!(mem::size_of::<AcceptedSlot<'static>>() == mem::size_of::<FixedLease<'static>>());
const _: () =
    assert!(mem::size_of::<CreatedSlot<'static>>() == mem::size_of::<Descriptor<'static>>());
const _: () =
    assert!(mem::size_of::<AcceptedSlot<'static>>() <= mem::size_of::<io::RecvEvent<'static>>());

pub struct Descriptor<'d> {
    lease: FixedLease<'d>,
}

/// A fixed descriptor whose driver's receive pool has been validated for
/// datagram metadata and at least one payload byte.
///
/// This exact-descriptor proof cannot cross driver lifetimes or fixed slots.
///
/// ```compile_fail
/// use dope_core::io::fd::handles::DatagramDescriptor;
///
/// fn shorten<'long: 'short, 'short>(
///     descriptor: DatagramDescriptor<'long>,
/// ) -> DatagramDescriptor<'short> {
///     descriptor
/// }
/// ```
#[repr(transparent)]
pub struct DatagramDescriptor<'d>(Descriptor<'d>);

/// An exclusive outbound fixed slot which does not contain a socket yet.
pub struct SocketSlot<'d> {
    lease: FixedLease<'d>,
}

/// An outbound fixed slot whose socket creation is in flight.
///
/// Its invariant driver lifetime cannot be shortened into another scope.
///
/// ```compile_fail
/// use dope_core::io::fd::handles::CreatingSocket;
///
/// fn shorten<'long: 'short, 'short>(creating: CreatingSocket<'long>) -> CreatingSocket<'short> {
///     creating
/// }
/// ```
pub struct CreatingSocket<'d> {
    lease: FixedLease<'d>,
    flight: flight::Flight<'d>,
}

const _: () =
    assert!(mem::size_of::<Descriptor<'static>>() == mem::size_of::<FixedLease<'static>>());
const _: () = {
    assert!(mem::size_of::<DatagramDescriptor<'static>>() == mem::size_of::<Descriptor<'static>>());
    assert!(
        mem::align_of::<DatagramDescriptor<'static>>() == mem::align_of::<Descriptor<'static>>()
    );
};
const _: () =
    assert!(mem::size_of::<SocketSlot<'static>>() == mem::size_of::<Descriptor<'static>>());
const _: () = assert!(
    mem::size_of::<CreatingSocket<'static>>()
        == mem::size_of::<(Descriptor<'static>, flight::Flight<'static>)>()
);

impl fmt::Debug for Descriptor<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Descriptor")
            .field(&self.lease.slot().raw())
            .finish()
    }
}

impl fmt::Debug for DatagramDescriptor<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DatagramDescriptor")
            .field(&self.index())
            .finish()
    }
}

impl<'d> DatagramDescriptor<'d> {
    pub(crate) fn validated(descriptor: Descriptor<'d>) -> Self {
        Self(descriptor)
    }

    pub fn into_descriptor(self) -> Descriptor<'d> {
        self.0
    }
}

impl<'d> From<DatagramDescriptor<'d>> for Descriptor<'d> {
    fn from(descriptor: DatagramDescriptor<'d>) -> Self {
        descriptor.into_descriptor()
    }
}

impl<'d> ops::Deref for DatagramDescriptor<'d> {
    type Target = Descriptor<'d>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for SocketSlot<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SocketSlot")
            .field(&self.lease.slot().raw())
            .finish()
    }
}

impl fmt::Debug for CreatingSocket<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CreatingSocket")
            .field(&self.lease.slot().raw())
            .finish()
    }
}

impl<'d> AcceptedSlot<'d> {
    pub fn bind(self) -> Descriptor<'d> {
        let Self { lease } = self;
        Descriptor { lease }
    }
}

impl<'d> CreatedSlot<'d> {
    fn matches(&self, lease: &FixedLease<'d>) -> bool {
        self.driver.same_driver(lease.driver()) && self.slot.raw() == lease.slot().raw()
    }

    /// Joins the two affine halves of a completed socket creation.
    /// Mismatch preserves both authorities for their original owners.
    pub fn activate(
        self,
        socket: CreatingSocket<'d>,
    ) -> Result<Descriptor<'d>, (CreatingSocket<'d>, Self)> {
        if !self.matches(&socket.lease) {
            return Err((socket, self));
        }
        let slot = socket.lease.slot();
        if !socket
            .lease
            .driver()
            .outbound()
            .activate_outbound(self.outbound, slot)
        {
            return Err((socket, self));
        }
        let CreatingSocket { lease, flight } = socket;
        let _created = mem::ManuallyDrop::new(self);
        let _ = flight.complete();
        Ok(Descriptor { lease })
    }
}

impl<'d> Descriptor<'d> {
    pub(crate) fn from_reserved_slot(
        slot: fixed::Slot<'d>,
        driver: driver::Reference<'d>,
    ) -> Option<Self> {
        Some(Self {
            lease: FixedLease::from_reserved(slot, driver)?,
        })
    }

    pub(crate) fn slot(&self) -> FixedSlot {
        self.lease.slot()
    }

    pub(crate) fn slot_ref(&self) -> &FixedSlot {
        self.lease.slot_ref()
    }

    pub fn index(&self) -> u32 {
        self.lease.slot().raw()
    }

    pub fn token_index(&self) -> route::SlotIndex {
        self.lease.slot().token_index()
    }

    pub fn driver(&self) -> driver::Reference<'d> {
        self.lease.driver()
    }

    pub fn ready_handle(&self) -> ready::Handle<'d> {
        self.lease.ready_handle()
    }

    pub(crate) fn into_lease(self) -> FixedLease<'d> {
        let Self { lease } = self;
        lease
    }

    pub(crate) fn close(self, backend: &mut backend::Backend) {
        use fixed::Lifecycle;

        let lease = self.into_lease();
        let driver = lease.driver();
        let released = lease.release();
        let slot = released.slot();
        match driver.files().fixed_owner(slot) {
            driver::FixedOwner::Accepted => {
                Lifecycle::close(
                    backend,
                    driver::Close::untracked(released.into_slot()),
                    driver,
                    fixed::Phase::Active,
                );
            }
            driver::FixedOwner::Outbound(outbound) => {
                match driver
                    .outbound()
                    .close_disposition(released.into_slot(), Some(outbound))
                {
                    driver::CloseDisposition::Submit(close) => {
                        Lifecycle::close(backend, close, driver, fixed::Phase::Active);
                    }
                    driver::CloseDisposition::NoSubmit(Some(retired)) => {
                        let slots = driver.outbound().take_retired_slots(retired);
                        Lifecycle::release_slots(backend, slots);
                    }
                    driver::CloseDisposition::NoSubmit(None) => {}
                }
            }
            driver::FixedOwner::Reserved => {
                let retired = fixed::Retirement::from_release(released);
                Lifecycle::retire(backend, retired.into_slot(), fixed::Phase::Active);
            }
        }
    }
}

impl<'d> SocketSlot<'d> {
    pub(crate) fn from_outbound_slot(
        slot: FixedSlot,
        driver: driver::Reference<'d>,
    ) -> Option<Self> {
        let Some(lease) = FixedLease::claim(slot, driver) else {
            if let Some(slots) = driver.outbound().release_outbound_slot_for(slot) {
                driver.maintenance().defer_outbound_slots(slots);
            }
            return None;
        };
        Some(Self { lease })
    }

    pub(crate) fn slot_ref(&self) -> &FixedSlot {
        self.lease.slot_ref()
    }

    pub fn index(&self) -> u32 {
        self.lease.slot().raw()
    }

    pub(crate) fn into_creating(self, flight: flight::Flight<'d>) -> CreatingSocket<'d> {
        let slot = self.lease.slot();
        if !self
            .lease
            .driver()
            .outbound()
            .begin_outbound_create_for(slot)
        {
            process::abort();
        }
        let lease = self.lease;
        CreatingSocket { lease, flight }
    }
}

impl<'d> CreatingSocket<'d> {
    pub fn driver(&self) -> driver::Reference<'d> {
        self.lease.driver()
    }

    pub fn ready_handle(&self) -> ready::Handle<'d> {
        self.lease.ready_handle()
    }
}
impl Drop for CreatedSlot<'_> {
    fn drop(&mut self) {
        self.driver
            .maintenance()
            .defer_descriptor(self.slot, Some(self.outbound));
    }
}
