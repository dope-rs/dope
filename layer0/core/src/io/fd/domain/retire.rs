use std::{marker, mem, num::NonZero};

use dope_sys::table::{
    reclaim,
    reclaim::{Item, Kind},
    token::host,
};
use o3::{
    base::{marker::ThreadBound, num},
    mem::{Lease, Return, lease::Open},
};
use resident::retire::{Retirement, Reuse};

use crate::{
    driver::{
        route,
        route::{indices, indices::SlotIndex},
    },
    io::fd::{
        domain,
        domain::{resident, state},
        handles, identity,
    },
};

const GENERATION_SHIFT: u32 = route::SLOT_BITS;
const RETIRE_BIT: u64 = 1 << (GENERATION_SHIFT + u32::BITS);
const SIDE_BIT: u64 = RETIRE_BIT << 1;
const RETIREMENT_MASK: u64 = RETIRE_BIT - 1;
const ITEM_MASK: u64 = RETIREMENT_MASK | SIDE_BIT;
const PRESENT_BIT: u64 = 1 << 63;

/// The last successful native accepted-close echo in this descriptor domain.
/// Physical slots and exhausted generations never reissue a completed identity.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct Completed(Option<NonZero<u64>>);

impl Completed {
    pub(crate) const EMPTY: Self = Self(None);

    fn new(inner: &Retirement) -> Self {
        Self(NonZero::new(pack(inner) | PRESENT_BIT))
    }
}

#[must_use = "a matched descriptor echo owns its retirement until consumed or retried"]
pub(crate) enum Bind<'d> {
    Ready(Echo<'d>),
    Missing,
    Class {
        echo: Echo<'d>,
        expected: handles::Retirement,
        observed: handles::Retirement,
    },
}

#[must_use = "a decoded descriptor return must be bound to its owning domain"]
#[repr(transparent)]
pub(crate) struct Returned(u64);

impl From<u64> for Returned {
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

#[must_use = "a pending descriptor transaction must be submitted or left queued"]
#[repr(transparent)]
pub(crate) struct Pending<'d>(u64, marker::PhantomData<fn(&'d ()) -> &'d ()>, ThreadBound);

#[must_use = "a classified close must be submitted or left queued"]
#[repr(transparent)]
pub(crate) struct Close<'d>(u64, marker::PhantomData<fn(&'d ()) -> &'d ()>, ThreadBound);

#[must_use = "a classified file-table retirement must be submitted or left queued"]
#[repr(transparent)]
pub(crate) struct Retire<'d>(u64, marker::PhantomData<fn(&'d ()) -> &'d ()>, ThreadBound);

#[must_use = "a submitted descriptor retirement must be recorded"]
#[repr(transparent)]
pub(crate) struct Flight<'d>(u64, marker::PhantomData<fn(&'d ()) -> &'d ()>, ThreadBound);

#[must_use = "a returned close echo must complete or retry its submitted retirement"]
pub(crate) struct Echo<'d> {
    inner: Lease<Retry<'d>, Open>,
}

struct Bound(u64);

struct Retry<'d>(View<'d>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct View<'a> {
    pub(super) domain: &'a domain::Domain,
    pub(super) thread: ThreadBound,
}

impl<'a> View<'a> {
    pub(crate) fn retire(&self, id: identity::Identity<'_>) {
        let release = self.domain.resident.retire().release(id);
        let slot = id.slot();
        if !self.domain.accept.contains(SlotIndex::from(slot))
            && let Some(owner) = slot.owner()
        {
            let slot = handles::OutboundSlot::new(SlotIndex::from(slot), owner);
            domain::Closures::from(self.domain).direct(slot);
        }
        release.enqueue();
    }

    pub(crate) fn unopened(&self, id: identity::Identity<'_, handles::OutboundSlot>) {
        let release = self.domain.resident.retire().release(id.physical());
        release.release();
        self.domain.release_slot(id.slot());
    }

    pub(crate) fn creating(&self, id: identity::Identity<'_, handles::OutboundSlot>) {
        let release = self.domain.resident.retire().release(id.physical());
        match domain::Closures::from(self.domain).creating(id) {
            domain::Close::Submit => release.enqueue(),
            domain::Close::Settled => release.release(),
            domain::Close::Released => {
                release.release();
                self.domain.release_slot(id.slot());
            }
        }
    }

    pub(crate) fn abandon(&self, accepted: handles::Accepted) -> handles::Reclaimed {
        self.domain.resident.retire().abandon(accepted)
    }

    pub(crate) fn discard(&self, slot: handles::OutboundSlot) {
        match domain::Closures::from(self.domain).created(slot) {
            domain::Close::Submit => self.domain.resident.retire().enqueue(slot.physical()),
            domain::Close::Settled => {}
            domain::Close::Released => self.domain.release_slot(slot),
        }
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.domain.resident.retire().pending().is_some()
    }

    pub(crate) fn has_inflight(&self) -> bool {
        self.domain.resident.retire().has_inflight()
    }

    pub(crate) fn completed(&self) -> Completed {
        self.domain.closed.get()
    }

    pub(crate) fn bind(self, returned: Returned) -> Bind<'a> {
        let raw = returned.0;
        if raw & !(ITEM_MASK | RETIRE_BIT) != 0 {
            return Bind::Missing;
        }
        let Some(inner) = self.domain.resident.retire().bind(unpack(raw & ITEM_MASK)) else {
            return Bind::Missing;
        };
        let observed = if raw & RETIRE_BIT == 0 {
            handles::Retirement::Close
        } else {
            handles::Retirement::Standalone
        };
        let expected = self.class(&inner);
        let class = if matches!(expected, handles::Retirement::Standalone) {
            RETIRE_BIT
        } else {
            0
        };
        let echo = Echo {
            inner: Lease::open(Retry(self), Bound(pack(&inner) | class)),
        };
        if expected == observed {
            Bind::Ready(echo)
        } else {
            Bind::Class {
                echo,
                expected,
                observed,
            }
        }
    }

    pub(crate) fn pending(self) -> Option<Pending<'a>> {
        let inner = self.domain.resident.retire().pending()?;
        let class = self.class(&inner);
        Some(Pending::new(inner, class))
    }

    fn class(&self, inner: &Retirement) -> handles::Retirement {
        let slot = handles::PhysicalSlot::from_index(inner.index());
        if self.domain.accept.contains(SlotIndex::from(slot)) {
            return handles::Retirement::Close;
        }
        let state = state::get(self.domain, slot).get();
        if state.owner().is_some() {
            handles::Retirement::Close
        } else {
            handles::Retirement::Standalone
        }
    }
}

impl<'d> Pending<'d> {
    fn new(inner: Retirement, class: handles::Retirement) -> Self {
        let packed = pack(&inner)
            | if class == handles::Retirement::Standalone {
                RETIRE_BIT
            } else {
                0
            };
        Self(packed, marker::PhantomData, ThreadBound::NEW)
    }

    pub(crate) fn classify(self) -> Kind<Close<'d>, Retire<'d>> {
        let raw = self.0;
        if raw & RETIRE_BIT == 0 {
            Kind::Close(Close(raw, self.1, self.2))
        } else {
            Kind::Retire(Retire(raw & ITEM_MASK, self.1, self.2))
        }
    }
}

impl<'d> reclaim::View for View<'d> {
    type Slot = handles::PhysicalSlot;
    type Flight = Flight<'d>;
    type Close = Close<'d>;
    type Retire = Retire<'d>;

    fn submitted(&self, flight: Self::Flight) {
        self.domain.resident.retire().submitted(flight.into_inner());
    }

    fn commit(self, item: Kind<Self::Close, Self::Retire>) {
        let view = self;
        match item {
            Kind::Close(close) => {
                let inner = unpack(close.0);
                let slot = handles::PhysicalSlot::from_index(inner.index());
                view.domain.resident.retire().commit(inner);
                if !view.domain.accept.contains(SlotIndex::from(slot)) {
                    domain::Closures::from(view.domain).complete(slot);
                }
            }
            Kind::Retire(retire) => {
                let (inner, solo) = retire.into_parts();
                if let Reuse::Reusable = view.domain.resident.retire().recycle(inner) {
                    domain::Outbound::from(view.domain).release_one(solo);
                }
            }
        }
    }
}

impl AsRef<u64> for Close<'_> {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl<'d> Item for Close<'d> {
    type Slot = handles::PhysicalSlot;
    type Flight = Flight<'d>;

    fn slot(&self) -> Self::Slot {
        slot(self.0)
    }

    fn submit(self) -> Self::Flight {
        Flight(self.0, self.1, self.2)
    }
}

impl host::Retire for Retire<'_> {
    fn echo(&self) -> u64 {
        self.0 | RETIRE_BIT
    }
}

impl<'d> Item for Retire<'d> {
    type Slot = handles::PhysicalSlot;
    type Flight = Flight<'d>;

    fn slot(&self) -> Self::Slot {
        slot(self.0)
    }

    fn submit(self) -> Self::Flight {
        Flight(self.0, self.1, self.2)
    }
}

impl Retire<'_> {
    fn into_parts(self) -> (Retirement, domain::Solo) {
        let inner = unpack(self.0);
        let solo = domain::Solo(handles::PhysicalSlot::from_index(inner.index()));
        (inner, solo)
    }
}

impl Flight<'_> {
    fn into_inner(self) -> Retirement {
        unpack(self.0)
    }
}

impl host::Echo for Echo<'_> {
    fn complete(self) {
        let (Retry(view), Bound(raw)) = self.inner.into_parts();
        let inner = unpack(raw & ITEM_MASK);
        let slot = handles::PhysicalSlot::from_index(inner.index());
        if raw & RETIRE_BIT != 0 {
            let reuse = view.domain.resident.retire().finish_reuse(inner);
            if let Reuse::Reusable = reuse {
                domain::Outbound::from(view.domain).release_one(domain::Solo(slot));
            }
            return;
        }
        let accepted = view.domain.accept.contains(inner.index());
        if accepted {
            view.domain.closed.set(Completed::new(&inner));
        }
        view.domain.resident.retire().finish(inner);
        if accepted {
            return;
        }
        domain::Closures::from(view.domain).complete(slot);
    }

    fn retry(self) {
        drop(self);
    }
}

impl Return for Retry<'_> {
    type Item = Bound;

    fn return_item(self, bound: Self::Item) {
        let inner = unpack(bound.0 & ITEM_MASK);
        self.0.domain.resident.retire().retry(inner);
    }
}

fn pack(inner: &Retirement) -> u64 {
    let side = match inner.side() {
        resident::Side::Zero => 0,
        resident::Side::One => SIDE_BIT,
    };
    (u64::from(inner.generation().get()) << GENERATION_SHIFT)
        | u64::from(inner.index().raw())
        | side
}

fn unpack(raw: u64) -> Retirement {
    let side = if raw & SIDE_BIT == 0 {
        resident::Side::Zero
    } else {
        resident::Side::One
    };
    Retirement::new(
        indices::SlotIndex::from_field(raw),
        num::Generation::from_raw((raw >> GENERATION_SHIFT) as u32),
        side,
    )
}

fn slot(raw: u64) -> handles::PhysicalSlot {
    handles::PhysicalSlot::from_index(indices::SlotIndex::from_field(raw))
}

const _: () = {
    assert!(PRESENT_BIT & (ITEM_MASK | RETIRE_BIT) == 0);
    assert!(mem::size_of::<Completed>() == mem::size_of::<u64>());
    assert!(SIDE_BIT & (RETIREMENT_MASK | RETIRE_BIT) == 0);
    assert!(Returned(ITEM_MASK | RETIRE_BIT).0 == ITEM_MASK | RETIRE_BIT);
    assert!(mem::size_of::<Returned>() == mem::size_of::<u64>());
    assert!(mem::align_of::<Returned>() == mem::align_of::<u64>());
    assert!(mem::size_of::<Option<Returned>>() == mem::size_of::<Option<u64>>());
    assert!(mem::size_of::<Pending<'_>>() == mem::size_of::<u64>());
    assert!(mem::align_of::<Pending<'_>>() == mem::align_of::<u64>());
    assert!(mem::size_of::<Option<Pending<'_>>>() == mem::size_of::<Option<u64>>());
    assert!(mem::size_of::<Close<'_>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Retire<'_>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Kind<Close<'_>, Retire<'_>>>() == 2 * mem::size_of::<u64>());
    assert!(mem::size_of::<Flight<'_>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Echo<'_>>() == mem::size_of::<(View<'_>, Retirement)>());
    assert!(mem::size_of::<Option<Flight<'_>>>() == mem::size_of::<Option<u64>>());
    assert!(mem::size_of::<View<'_>>() == mem::size_of::<usize>());
};
