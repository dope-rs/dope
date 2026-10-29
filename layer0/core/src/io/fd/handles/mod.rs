use std::{marker, mem, ops};

use dope_sys::engine::life::Resolver as _;
use o3::mem::{Lease, Return, lease::Open};

use crate::driver::route::indices;

mod creation;

pub use creation::Creation;

use crate::{
    driver,
    driver::{
        retained::flight,
        route,
        route::{kind::Socket, table},
        schedule::ready,
    },
    io::{
        event::net::recv,
        fd::{domain, identity, lease, slots},
        socket::{install::receive, pending},
    },
};

const OWNER_SHIFT: u32 = route::SLOT_BITS;
const UNOWNED: u8 = route::FRAMEWORK;
type Invariant<'d> = marker::PhantomData<*mut &'d ()>;

/// A physical slot whose outbound owner is established by construction.
/// It preserves the ownership proof without decoding packed owner bits again.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct OwnedSlot(PhysicalSlot);

#[derive(Clone, Copy)]
#[repr(C, align(4))]
pub struct OutboundSlot {
    index: [u8; 3],
    owner: route::OutboundKey,
}

#[must_use = "accepted descriptor authority must be delivered or reclaimed"]
#[repr(transparent)]
pub struct Accepted(PhysicalSlot);

pub(crate) enum Admission<T, R = ()> {
    Ready(T),
    Retired(R),
}

#[must_use = "created descriptor authority must be delivered or reclaimed"]
#[repr(transparent)]
pub(crate) struct Created<'d> {
    slot: OutboundSlot,
    driver: Invariant<'d>,
}

impl From<OutboundSlot> for OwnedSlot {
    fn from(slot: OutboundSlot) -> Self {
        Self(slot.physical())
    }
}

impl From<&slots::ReservedSlot<'_>> for OwnedSlot {
    fn from(slot: &slots::ReservedSlot<'_>) -> Self {
        Self(slot.physical())
    }
}

impl From<OwnedSlot> for PhysicalSlot {
    fn from(slot: OwnedSlot) -> Self {
        slot.0
    }
}

impl OutboundSlot {
    pub(in crate::io::fd) fn new(index: indices::SlotIndex, owner: route::OutboundKey) -> Self {
        Self {
            index: index.into_le24(),
            owner,
        }
    }

    pub(crate) fn physical(self) -> PhysicalSlot {
        PhysicalSlot(
            (self.owner.raw() << OWNER_SHIFT)
                | u32::from_le_bytes([self.index[0], self.index[1], self.index[2], 0]),
        )
    }

    fn is_same(self, other: PhysicalSlot) -> bool {
        self.physical().0 == other.0
    }

    pub(in crate::io::fd) const fn owner(self) -> route::OutboundKey {
        self.owner
    }
}

impl Accepted {
    pub(crate) fn bind<'d>(
        self,
        driver: driver::Reference<'d>,
    ) -> Admission<AcceptedSlot<'d>, Reclaimed> {
        let slot = self.0;
        let Some(id) = driver.descriptors().domain().claim(slot) else {
            return Admission::Retired(driver.descriptors().abandon(self));
        };
        Admission::Ready(AcceptedSlot {
            lease: lease::Lease::<lease::Active>::from_claim(id, driver),
        })
    }

    pub(crate) fn physical(&self) -> PhysicalSlot {
        self.0
    }
}

impl<'d> Created<'d> {
    pub(crate) fn complete(
        creation: domain::Creation,
        driver: driver::Reference<'d>,
    ) -> Admission<Self> {
        let slot = *creation.as_ref();
        match driver.outbound().complete(creation) {
            domain::Create::Deliver => Admission::Ready(Self {
                slot,
                driver: marker::PhantomData,
            }),
            domain::Create::Retired => Admission::Retired(()),
        }
    }

    pub(in crate::io) fn bind(self, driver: driver::Reference<'d>) -> CreatedSlot<'d> {
        CreatedSlot {
            inner: Lease::open(Discard { driver }, self.slot),
        }
    }
}

#[repr(transparent)]
pub struct AcceptedSlot<'d> {
    lease: lease::Lease<'d, lease::Active>,
}

#[must_use = "created socket must be activated or reclaimed"]
#[repr(C)]
/// The successful kernel half of one driver-scoped socket creation.
///
/// Its invariant driver lifetime cannot be shortened into another scope.
///
pub struct CreatedSlot<'d> {
    inner: Lease<Discard<'d>, Open>,
}

/// A validated pair of the kernel and submission halves of one socket creation.
pub struct Join<'d, State = pending::Live> {
    made: CreatedSlot<'d>,
    creating: Creation<'d, State>,
}

struct Discard<'d> {
    driver: driver::Reference<'d>,
}

pub struct Descriptor<'d> {
    lease: lease::Lease<'d, lease::Active>,
}

/// A descriptor whose driver's receive pool has been validated for
/// datagram metadata and at least one payload byte.
///
/// This exact-descriptor proof cannot cross driver lifetimes or physical slots.
///
#[repr(transparent)]
pub struct Datagram<'d>(Descriptor<'d>);

/// An exclusive outbound descriptor slot which does not contain a socket yet.
pub struct SocketSlot<'d> {
    lease: lease::Lease<'d, lease::Unopened>,
}

impl<'d> Datagram<'d> {
    pub(crate) fn new(fd: Descriptor<'d>, _: receive::Capacity<'d>) -> Self {
        Self(fd)
    }
}

impl<'d> From<Datagram<'d>> for Descriptor<'d> {
    fn from(value: Datagram<'d>) -> Self {
        value.0
    }
}

impl<'d> ops::Deref for Datagram<'d> {
    type Target = Descriptor<'d>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'d> AcceptedSlot<'d> {
    pub fn bind(self) -> Descriptor<'d> {
        let Self { lease } = self;
        Descriptor { lease }
    }
}

impl<'d> CreatedSlot<'d> {
    pub fn try_join<State>(
        self,
        creating: Creation<'d, State>,
    ) -> Result<Join<'d, State>, (Creation<'d, State>, Self)> {
        if !self.inner.item().is_same(creating.lease.slot()) {
            return Err((creating, self));
        }
        Ok(Join {
            made: self,
            creating,
        })
    }
}

impl<'d, State> Join<'d, State> {
    pub fn activate(self) -> Descriptor<'d> {
        let Self { made, creating } = self;
        let (discard, made) = made.inner.into_parts();
        let Discard { driver } = discard;
        driver.outbound().activate(made);
        let lease = creating.lease.activate();
        Descriptor { lease }
    }
}

impl<'d> Descriptor<'d> {
    pub fn close(self, driver: &mut driver::Context<'_, 'd>) {
        drop(self);
        driver.native().resolve().retire();
    }

    pub(crate) fn from_slot(slot: slots::ReservedSlot<'d>, driver: driver::Reference<'d>) -> Self {
        Self {
            lease: lease::Lease::from_slot(slot, driver),
        }
    }

    pub(crate) fn slot(&self) -> PhysicalSlot {
        self.lease.slot()
    }

    pub(crate) fn slot_ref(&self) -> &PhysicalSlot {
        self.lease.slot_ref()
    }

    pub fn index(&self) -> indices::SlotIndex {
        self.lease.slot().into()
    }

    pub fn driver(&self) -> driver::Reference<'d> {
        self.lease.driver()
    }

    pub fn ready_handle(&self) -> ready::Handle<'d> {
        self.lease.ready_handle()
    }
}

impl<'d> SocketSlot<'d> {
    pub(crate) fn from_claim(
        id: identity::Identity<'d, OutboundSlot>,
        driver: driver::Reference<'d>,
    ) -> Self {
        Self {
            lease: lease::Lease::<lease::Unopened>::from_claim(id, driver),
        }
    }

    pub(crate) fn outbound(&self) -> OutboundSlot {
        self.lease.outbound()
    }

    pub(crate) fn begin(self, flight: flight::Flight<'d, Socket>) -> Creation<'d> {
        let lease = self.lease.begin();
        Creation {
            lease,
            flight,
            state: marker::PhantomData,
        }
    }
}

impl Return for Discard<'_> {
    type Item = OutboundSlot;

    fn return_item(self, slot: Self::Item) {
        self.driver.descriptors().discard(slot);
    }
}

const _: () = {
    assert!(mem::size_of::<PhysicalSlot>() == mem::size_of::<u32>());
    assert!(mem::size_of::<OwnedSlot>() == mem::size_of::<PhysicalSlot>());
    assert!(mem::size_of::<[u8; 3]>() * u8::BITS as usize == route::SLOT_BITS as usize);
    assert!(mem::align_of::<[u8; 3]>() == mem::align_of::<u8>());
    assert!(mem::size_of::<OutboundSlot>() == mem::size_of::<PhysicalSlot>());
    assert!(mem::align_of::<PhysicalSlot>() == mem::align_of::<u32>());
    assert!(mem::align_of::<OutboundSlot>() == mem::align_of::<PhysicalSlot>());
    assert!(mem::size_of::<Option<PhysicalSlot>>() == mem::size_of::<Option<u32>>());
};
const _: () = assert!(mem::size_of::<Retirement>() == mem::size_of::<u8>());
const _: () = assert!(mem::size_of::<Accepted>() == mem::size_of::<PhysicalSlot>());
const _: () = {
    assert!(mem::size_of::<Invariant<'static>>() == 0);
    assert!(mem::align_of::<Invariant<'static>>() == 1);
    assert!(mem::size_of::<Created<'static>>() == mem::size_of::<OutboundSlot>());
    assert!(mem::align_of::<Created<'static>>() == mem::align_of::<OutboundSlot>());
};
const _: () = {
    assert!(
        mem::size_of::<Admission<AcceptedSlot<'_>>>() == mem::size_of::<Option<AcceptedSlot<'_>>>()
    );
    assert!(mem::size_of::<Admission<AcceptedSlot<'_>>>() == mem::size_of::<AcceptedSlot<'_>>());
    assert!(mem::align_of::<Admission<AcceptedSlot<'_>>>() == mem::align_of::<AcceptedSlot<'_>>());
};
const _: () = assert!(
    mem::size_of::<AcceptedSlot<'_>>() == mem::size_of::<lease::Lease<'_, lease::Active>>()
);
const _: () = {
    assert!(mem::size_of::<CreatedSlot<'_>>() == mem::size_of::<Descriptor<'_>>());
    assert!(mem::align_of::<CreatedSlot<'_>>() == mem::align_of::<Descriptor<'_>>());
    assert!(mem::size_of::<Join<'_>>() == mem::size_of::<(CreatedSlot<'_>, Creation<'_>)>());
    assert!(mem::align_of::<Join<'_>>() == mem::align_of::<(CreatedSlot<'_>, Creation<'_>)>());
    assert!(
        mem::size_of::<Admission<CreatedSlot<'_>>>() == mem::size_of::<Option<CreatedSlot<'_>>>()
    );
    assert!(mem::size_of::<Admission<CreatedSlot<'_>>>() == mem::size_of::<CreatedSlot<'_>>());
    assert!(mem::align_of::<Admission<CreatedSlot<'_>>>() == mem::align_of::<CreatedSlot<'_>>());
};
const _: () = assert!(mem::size_of::<AcceptedSlot<'_>>() <= mem::size_of::<recv::Outcome<'_>>());
const _: () =
    assert!(mem::size_of::<Descriptor<'_>>() == mem::size_of::<lease::Lease<'_, lease::Active>>());
const _: () = {
    assert!(mem::size_of::<Datagram<'_>>() == mem::size_of::<Descriptor<'_>>());
    assert!(mem::align_of::<Datagram<'_>>() == mem::align_of::<Descriptor<'_>>());
    assert!(mem::size_of::<Option<Datagram<'_>>>() == mem::size_of::<Option<Descriptor<'_>>>());
};
const _: () = {
    assert!(
        mem::size_of::<Creation<'_, pending::Cancelled>>()
            == mem::size_of::<Creation<'_, pending::Live>>()
    );
    assert!(
        mem::size_of::<Creation<'_>>()
            == mem::size_of::<lease::Lease<'_, lease::Creating>>()
                + mem::size_of::<flight::Flight<'_, Socket>>()
    );
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
/// Descriptor-table identity observed after its operation has been reclaimed.
pub struct PhysicalSlot(u32);

/// The descriptor's destination after its accepted delivery was relinquished.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reclaimed {
    Queued(PhysicalSlot),
    /// The native file table owns this slot until reactor teardown.
    Quarantined(PhysicalSlot),
}

/// Descriptor-table ownership class observed on a retirement completion.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum Retirement {
    Close,
    Standalone,
}

impl PhysicalSlot {
    pub(in crate::io::fd) const fn from_index(index: indices::SlotIndex) -> Self {
        Self(((UNOWNED as u32) << OWNER_SHIFT) | index.raw())
    }

    pub(crate) fn owner(self) -> Option<route::OutboundKey> {
        route::OutboundKey::from_raw(self.0 >> OWNER_SHIFT)
    }

    pub fn route(self) -> route::Route {
        route::Route::from_bits((self.0 >> OWNER_SHIFT) as u8)
    }
}

impl From<Accepted> for PhysicalSlot {
    fn from(accepted: Accepted) -> Self {
        accepted.0
    }
}

impl From<PhysicalSlot> for indices::SlotIndex {
    fn from(slot: PhysicalSlot) -> Self {
        Self::from_field(u64::from(slot.0))
    }
}

impl TryFrom<(indices::SlotIndex, table::Capacity)> for Accepted {
    type Error = (indices::SlotIndex, table::Capacity);

    fn try_from((index, end): (indices::SlotIndex, table::Capacity)) -> Result<Self, Self::Error> {
        if end.contains(index) {
            Ok(Self(PhysicalSlot::from_index(index)))
        } else {
            Err((index, end))
        }
    }
}

impl From<PhysicalSlot> for [u8; 3] {
    fn from(slot: PhysicalSlot) -> Self {
        indices::SlotIndex::from(slot).into_le24()
    }
}

impl From<OutboundSlot> for [u8; 3] {
    fn from(slot: OutboundSlot) -> Self {
        slot.index
    }
}

impl From<PhysicalSlot> for u32 {
    fn from(slot: PhysicalSlot) -> Self {
        slot.0 & route::SLOT_MASK as u32
    }
}
