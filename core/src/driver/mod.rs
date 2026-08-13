pub mod flight;
pub mod lifecycle;
pub mod ops;
pub mod retained;
pub mod route;
pub mod schedule;
pub mod settings;
pub mod storage;

use std::{error, fmt, io, marker, mem, pin, time};

pub(crate) use lifecycle::Source;
use o3::{self, cell::region, collections::batch::set};
pub(crate) use ownership::{AccountedRecvOwner, RecvOwner};

use self::{lifecycle::quiesce, ops::access, route::kind, schedule::timer, storage::ownership};
use crate::{backend, io::fd::handles, platform};

#[derive(Debug, Clone, Copy)]
pub struct SubmitError;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecvCreditWake {
    ResourceReturned = kind::RECV_CREDIT_RETURNED,
    WaiterRetry = kind::RECV_CREDIT_RETRY,
}

/// One provided receive buffer made available to an exact waiting target.
/// Dropping an unused credit transfers it to the next waiter.
#[must_use = "a receive buffer credit must be consumed or returned"]
#[repr(transparent)]
pub struct RecvBufferCredit<'d>(Reference<'d>);

impl<'d> RecvBufferCredit<'d> {
    fn new(driver: Reference<'d>) -> Self {
        Self(driver)
    }

    pub fn consume(self) {
        let _consumed = mem::ManuallyDrop::new(self);
    }
}

impl Drop for RecvBufferCredit<'_> {
    fn drop(&mut self) {
        self.0.credits().release_recv_buffer();
    }
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dope: submission was not accepted")
    }
}

impl error::Error for SubmitError {}

impl From<SubmitError> for io::Error {
    fn from(_: SubmitError) -> Self {
        io::Error::from(io::ErrorKind::WouldBlock)
    }
}

type Buffer = <backend::Backend as platform::Buffer>::Token;

type Invariant<'d> = marker::PhantomData<fn(&'d ()) -> &'d ()>;

#[derive(Clone, Copy)]
pub(crate) enum FixedOwner {
    Accepted,
    Reserved,
    Outbound(OutboundKey),
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct OutboundKey(u8);

// SAFETY: OutboundKey's private byte is stable across copies. Retirement
// storage reconstructs only raw indices previously produced by this type.
impl set::DenseIndex for OutboundKey {
    fn into_usize(self) -> usize {
        self.raw() as usize
    }

    fn from_usize(raw: usize) -> Self {
        Self::from_bounded(raw as u8)
    }
}

#[must_use = "a fixed-file close must be submitted to the backend"]
#[repr(transparent)]
pub(crate) struct Close<'d> {
    slot: handles::FixedSlot,
    _brand: Invariant<'d>,
}

const _: () =
    assert!(std::mem::size_of::<Close<'static>>() == std::mem::size_of::<handles::FixedSlot>());
#[must_use = "an outbound authority release must be completed"]
pub(crate) enum CloseDisposition<'d> {
    Submit(Close<'d>),
    NoSubmit(Option<RetiredSlots<'d>>),
}

#[must_use = "a successful socket creation must be delivered or reclaimed"]
pub(crate) enum CreateSuccess<'d> {
    Deliver(OutboundKey),
    Close(Close<'d>),
}

#[must_use = "retired fixed slots must be returned to the backend allocator"]
#[repr(transparent)]
pub(crate) struct RetiredSlots<'d> {
    key: OutboundKey,
    _brand: Invariant<'d>,
}

const _: () =
    assert!(std::mem::size_of::<RetiredSlots<'static>>() == std::mem::size_of::<OutboundKey>());

impl OutboundKey {
    pub(crate) fn raw(self) -> u32 {
        self.0 as u32
    }

    pub(crate) const fn from_raw(raw: u32) -> Option<Self> {
        if raw < route::FRAMEWORK as u32 {
            Some(Self(raw as u8))
        } else {
            None
        }
    }

    const fn from_bounded(raw: u8) -> Self {
        debug_assert!(raw < route::FRAMEWORK);
        Self(raw)
    }

    const fn for_route<const ID: u8>() -> Option<Self> {
        if ID == route::FRAMEWORK {
            None
        } else {
            Some(Self(ID))
        }
    }
}

impl<'d> Close<'d> {
    pub(crate) fn untracked(slot: handles::FixedSlot) -> Self {
        Self {
            slot,
            _brand: marker::PhantomData,
        }
    }

    fn tracked(slot: handles::FixedSlot) -> Self {
        Self {
            slot,
            _brand: marker::PhantomData,
        }
    }

    pub(crate) fn into_slot(self) -> handles::FixedSlot {
        self.slot
    }
}

impl<'d> RetiredSlots<'d> {
    fn new(key: OutboundKey) -> Self {
        Self {
            key,
            _brand: marker::PhantomData,
        }
    }

    fn into_key(self) -> OutboundKey {
        self.key
    }

    fn key(&self) -> OutboundKey {
        self.key
    }
}

pub struct Driver {
    backend: backend::Backend,
    flights: flight::Arena,
    shared: access::Shared,
    timer_cache_limit: settings::ScheduleCapacity,
    _pin: marker::PhantomPinned,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Reference<'d> {
    pub(in crate::driver) shared: &'d access::Shared,
    _brand: Invariant<'d>,
}

impl<'d> Reference<'d> {
    pub(in crate::driver) fn new(shared: &'d access::Shared) -> Self {
        Self {
            shared,
            _brand: marker::PhantomData,
        }
    }

    pub const fn ready(self) -> access::Ready<'d> {
        access::Ready::new(self)
    }

    pub const fn targets<Tag: route::Tag>(self) -> route::Space<'d, Tag> {
        route::Space {
            driver: marker::PhantomData,
        }
    }

    pub(in crate::driver) const fn credits(self) -> access::Credits<'d> {
        access::Credits::new(self)
    }

    pub const fn scheduler(self) -> access::Scheduler<'d> {
        access::Scheduler::new(self)
    }

    pub fn maintenance_progress(self) -> schedule::Progress<'d> {
        if self.maintenance().has_deferred_maintenance() {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }

    pub(crate) const fn receive(self) -> access::Receive<'d> {
        access::Receive::new(self)
    }

    pub(crate) const fn maintenance(self) -> access::Maintenance<'d> {
        access::Maintenance::new(self)
    }

    pub(crate) const fn files(self) -> access::Files<'d> {
        access::Files::new(self)
    }

    pub(crate) const fn outbound(self) -> access::Outbound<'d> {
        access::Outbound::new(self)
    }

    pub(crate) fn same_driver(self, other: Self) -> bool {
        (self.shared as *const access::Shared).addr()
            == (other.shared as *const access::Shared).addr()
    }
}

const _: () = assert!(mem::size_of::<Reference<'static>>() == mem::size_of::<usize>());

pub struct Context<'a, 'd> {
    driver: Reference<'d>,
    backend: &'a mut backend::Backend,
    flights: &'a mut flight::Arena,
    region: &'a mut region::Token<'d>,
    timer: &'d timer::Timer<'d>,
}

impl<'a, 'd> Context<'a, 'd> {
    fn new(
        driver: Reference<'d>,
        backend: &'a mut backend::Backend,
        flights: &'a mut flight::Arena,
        region: &'a mut region::Token<'d>,
        timer: &'d timer::Timer<'d>,
    ) -> Self {
        Self {
            driver,
            backend,
            flights,
            region,
            timer,
        }
    }

    pub fn reborrow(&mut self) -> Context<'_, 'd> {
        Context {
            driver: self.driver,
            backend: &mut *self.backend,
            flights: &mut *self.flights,
            region: &mut *self.region,
            timer: self.timer,
        }
    }

    pub fn region_token(&mut self) -> &mut region::Token<'d> {
        self.region
    }

    #[doc(hidden)]
    pub fn region_token_ref(&self) -> &region::Token<'d> {
        self.region
    }

    pub fn driver_ref(&self) -> Reference<'d> {
        self.driver
    }

    #[doc(hidden)]
    pub fn flight_slots<Tag: route::Tag>(
        &mut self,
        capacity: usize,
    ) -> io::Result<flight::Slots<'d, Tag>> {
        self.flights.try_slots(capacity, self.driver)
    }

    pub(crate) fn backend_drain(&mut self) -> (&mut backend::Backend, flight::Drain<'_, 'd>) {
        let Self {
            driver,
            backend,
            flights,
            ..
        } = self;
        (backend, flight::Drain::new(flights, *driver))
    }

    pub fn timer(&self) -> &'d timer::Timer<'d> {
        self.timer
    }

    pub fn deadline_now(&self) -> timer::Deadline<'d> {
        self.driver.scheduler().deadline(self.turn_now())
    }

    pub fn turn_now(&self) -> time::Instant {
        self.driver.scheduler().turn_now()
    }

    pub(crate) fn backend(&mut self) -> &mut backend::Backend {
        self.backend
    }

    pub(crate) fn accept_slot_capacity(&self) -> usize {
        self.driver.files().accept_capacity()
    }

    pub(crate) fn outbound_slot_capacity(&self) -> usize {
        self.driver.files().outbound_capacity()
    }

    pub(crate) fn pop_returned_buffer(&self) -> Option<Buffer> {
        self.driver.maintenance().pop_returned_buffer()
    }
}

impl Driver {
    pub fn new(config: settings::Config) -> io::Result<Self> {
        config.validate_structure()?;
        let file_slots = config.file_slots();
        let receive = config.receive();
        let state = <backend::Backend as platform::Runtime>::build(&config)?;
        let scheduler = config.scheduler();
        let shared = access::Shared::try_new(file_slots, scheduler.ready(), receive)?;
        Ok(Self {
            backend: state,
            flights: flight::Arena::new(),
            shared,
            timer_cache_limit: scheduler.timer_cache_limit(),
            _pin: marker::PhantomPinned,
        })
    }

    pub fn scope<R>(
        self: pin::Pin<&mut Self>,
        owner: quiesce::Lease,
        f: impl for<'d> FnOnce(lifecycle::Scope<'d>) -> R,
    ) -> R {
        let timer_cache_limit = self.as_ref().get_ref().timer_cache_limit;
        lifecycle::Lease::take(self).run(timer_cache_limit, owner, f)
    }

    #[doc(hidden)]
    pub fn scope_with_storage<S, R>(
        self: pin::Pin<&mut Self>,
        owner: quiesce::Lease,
        factory: S,
        f: impl for<'d> FnOnce(lifecycle::Scope<'d>, pin::Pin<&'d S::Output<'d>>) -> R,
    ) -> Result<R, S::Error>
    where
        S: storage::Factory,
    {
        let timer_cache_limit = self.as_ref().get_ref().timer_cache_limit;
        lifecycle::Lease::take(self).run_with_storage(timer_cache_limit, owner, factory, f)
    }
}
