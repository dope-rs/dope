pub mod bootstrap;
pub mod buffers;
pub mod completion;
pub mod control;
pub mod datagram;
pub mod ext;
pub mod profile;
pub mod ready;
pub mod route;
pub mod submission;
pub mod token;

use std::cell::Cell;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, ErrorKind, Result};
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::time::Instant;

use o3::cell::{BrandToken, RegionToken};
use o3::collections::CellQueue;
use o3::marker::ThreadBound;

use crate::backend::Backend;
use crate::backend::ops::buffers::BufferBackend;
use crate::io::fd::{Fd, FdGuard, FdSlot};
use crate::io::provided::raw::buffer::BufferId;
use crate::platform::Platform;
use crate::platform::raw::file::{FileLimit, lock_memory_best_effort};
use profile::DriverProfile;
use ready::{Arena, ReadyHandle, ReadyKey, ReadySlot};
use token::{SlotIndex, Token, TokenCapacity};

type Invariant<'d> = PhantomData<fn(&'d ()) -> &'d ()>;

struct Shared {
    arena: Box<Arena>,
    returned_buffers: CellQueue<BufferId>,
    turn_clock: Cell<Instant>,
}

pub struct Driver {
    shared: Shared,
    backend: Backend,
    _pin: PhantomPinned,
}

pub struct DriverRef<'d> {
    shared: &'d Shared,
    _brand: Invariant<'d>,
}

pub struct Scope<'d> {
    driver: DriverRef<'d>,
    backend: &'d mut Backend,
    region: RegionToken<'d>,
    token: BrandToken<'d>,
}

pub struct DriverContext<'a, 'd> {
    driver: DriverRef<'d>,
    backend: &'a mut Backend,
    region: &'a mut RegionToken<'d>,
}

impl Driver {
    pub fn init_process() -> Result<()> {
        lock_memory_best_effort();
        FileLimit::get()?.raise()
    }

    pub(crate) fn from_state(
        state: Backend,
        fixed_slots: usize,
        dynamic_slots: usize,
        provided_buffers: usize,
    ) -> Result<Self> {
        Ok(Self {
            shared: Shared {
                arena: Arena::new(fixed_slots, dynamic_slots)?,
                returned_buffers: CellQueue::with_capacity(provided_buffers),
                turn_clock: Cell::new(Instant::now()),
            },
            backend: state,
            _pin: PhantomPinned,
        })
    }

    pub fn scope<R>(self: Pin<&mut Self>, f: impl for<'d> FnOnce(Scope<'d>) -> R) -> R {
        // SAFETY: the pointer is used only within the synchronous branded scope,
        // while the pinned exclusive borrow remains active.
        let this = unsafe { self.get_unchecked_mut() as *mut Self };
        BrandToken::scope_with_region(move |token, region| {
            // SAFETY: the higher-ranked closure prevents the generated lifetime
            // and every reference derived from this pointer from escaping.
            let this = unsafe { &mut *this };
            f(Scope {
                driver: DriverRef::new(&this.shared),
                backend: &mut this.backend,
                region,
                token,
            })
        })
    }
}

impl<'d> DriverRef<'d> {
    fn new(shared: &'d Shared) -> Self {
        Self {
            shared,
            _brand: PhantomData,
        }
    }

    fn arena(self) -> &'d Arena {
        &self.shared.arena
    }

    pub(crate) fn fixed_ready(self, slot: FdSlot) -> ReadyHandle<'d> {
        self.arena().fixed_slot(slot)
    }

    pub(crate) fn fixed_fd_slot(self, raw: u32) -> Option<FdSlot> {
        self.arena().fd_slot(raw)
    }

    pub fn make_ready_slot(self, target: Token) -> Result<ReadySlot<'d>> {
        self.arena().make_slot(target)
    }

    pub fn make_ready_slot_reserving(self, target: Token, reserve: usize) -> Result<ReadySlot<'d>> {
        self.arena().make_slot_reserving(target, reserve)
    }

    pub fn make_ready_slots<I>(self, targets: I) -> Result<Box<[ReadySlot<'d>]>>
    where
        I: IntoIterator<Item = Token>,
        I::IntoIter: ExactSizeIterator,
    {
        self.arena().make_slots(targets)
    }

    pub fn activate_ready(self, key: ReadyKey<'d>) {
        self.arena().activate(key);
    }

    #[doc(hidden)]
    pub fn arm_recv_credit(self, key: ReadyKey<'d>, target: Token) -> bool {
        self.arena().arm_recv_credit(key, target)
    }

    #[doc(hidden)]
    pub fn release_recv_credit(self, key: ReadyKey<'d>, target: Token) {
        self.arena().release_recv_credit(key, target);
    }

    #[doc(hidden)]
    pub fn take_recv_credit(self, key: ReadyKey<'d>, target: Token) -> bool {
        self.arena().take_recv_credit(key, target)
    }

    pub fn drain_ready(self, activate: impl FnMut(Token)) {
        self.arena().drain(activate);
    }

    pub fn has_ready(self) -> bool {
        self.arena().has_ready()
    }

    /// Returns the monotonic-clock snapshot for the current driver turn.
    ///
    /// Unlike [`Instant::now`], this is a cached read. The runtime refreshes it
    /// at completion-batch entry and immediately before preparing to park, so
    /// callbacks in one turn share a coherent time base without performing a
    /// clock read for every event.
    pub fn turn_now(self) -> Instant {
        self.shared.turn_clock.get()
    }

    pub(crate) fn return_buffer(self, id: BufferId) {
        assert!(
            self.shared.returned_buffers.push_back(id).is_ok(),
            "dope: provided-buffer return queue overflow"
        );
    }
}

impl Copy for DriverRef<'_> {}

impl Clone for DriverRef<'_> {
    fn clone(&self) -> Self {
        *self
    }
}

impl PartialEq for DriverRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        core::ptr::eq(self.shared, other.shared)
    }
}

impl Eq for DriverRef<'_> {}

impl<'d> Scope<'d> {
    pub fn context(&mut self) -> DriverContext<'_, 'd> {
        DriverContext {
            driver: self.driver,
            backend: &mut *self.backend,
            region: &mut self.region,
        }
    }

    pub fn token(&mut self) -> &mut BrandToken<'d> {
        &mut self.token
    }

    pub fn token_and_context(&mut self) -> (&mut BrandToken<'d>, DriverContext<'_, 'd>) {
        (
            &mut self.token,
            DriverContext {
                driver: self.driver,
                backend: &mut *self.backend,
                region: &mut self.region,
            },
        )
    }

    pub fn driver_ref(&self) -> DriverRef<'d> {
        self.driver
    }
}

impl<'a, 'd> DriverContext<'a, 'd> {
    pub fn reborrow(&mut self) -> DriverContext<'_, 'd> {
        DriverContext {
            driver: self.driver,
            backend: &mut *self.backend,
            region: &mut *self.region,
        }
    }

    pub fn region_token(&mut self) -> &mut RegionToken<'d> {
        self.region
    }

    #[doc(hidden)]
    pub fn region_token_ref(&self) -> &RegionToken<'d> {
        self.region
    }

    pub fn driver_ref(&self) -> DriverRef<'d> {
        self.driver
    }

    /// Returns the monotonic-clock snapshot for the current driver turn.
    pub fn turn_now(&self) -> Instant {
        self.driver.turn_now()
    }

    /// Starts a new driver-clock epoch and returns its snapshot.
    ///
    /// Runtimes should refresh once before dispatching a completion/ready batch
    /// and once after application callbacks, immediately before timeout
    /// expiration and park-duration calculations.
    #[doc(hidden)]
    pub fn refresh_turn_clock(&mut self) -> Instant {
        let now = Instant::now();
        self.driver.shared.turn_clock.set(now);
        now
    }

    pub(crate) fn backend(&mut self) -> &mut Backend {
        self.backend
    }

    pub(crate) fn backend_ref(&self) -> &Backend {
        self.backend
    }

    pub(crate) fn release_buffer(&mut self, id: BufferId) {
        <Backend as BufferBackend>::release_buffer(self.backend(), id);
    }

    pub(crate) fn flush_returned_buffers(&mut self) {
        while let Some(id) = self.driver.shared.returned_buffers.pop_front() {
            self.release_buffer(id);
        }
    }
    pub fn guard(&mut self, fd: Fd<'d>) -> FdGuard<'_, 'd> {
        let (slot, driver, retire_slot) = fd.into_parts();
        FdGuard::new(self.backend(), slot, driver, retire_slot)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProvidedBufferConfig {
    pub len: usize,
    pub entries: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub ring_entries: u32,
    pub cq_entries: u32,
    pub fixed_file_slots: u32,
    pub accept_slots: u32,
    pub provided: ProvidedBufferConfig,
    pub defer_taskrun: bool,
    pub ready_slots: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ring_entries: 1024,
            cq_entries: 2048,
            fixed_file_slots: 65536,
            accept_slots: 65536,
            provided: ProvidedBufferConfig {
                len: 4096,
                entries: 128,
            },
            defer_taskrun: false,
            ready_slots: 65536,
        }
    }
}

impl Config {
    const MAX_ENTRIES: u32 = 32768;

    pub fn fixed_file_slots(&self) -> u32 {
        self.fixed_file_slots
    }

    fn sized_sq(max_connections: u32, outbound_reserve: u32) -> u32 {
        max_connections
            .saturating_add(outbound_reserve)
            .next_power_of_two()
            .clamp(64, Self::MAX_ENTRIES)
    }

    pub fn for_profile<P: DriverProfile>() -> Self {
        Self {
            ring_entries: P::RING_ENTRIES,
            cq_entries: P::CQ_ENTRIES,
            fixed_file_slots: P::FIXED_FILE_SLOTS,
            accept_slots: P::FIXED_FILE_SLOTS.saturating_sub(P::OUTBOUND_RESERVE),
            provided: ProvidedBufferConfig {
                len: P::PROVIDED_BUF_LEN,
                entries: P::PROVIDED_BUF_ENTRIES,
            },
            defer_taskrun: P::DEFER_TASKRUN,
            ready_slots: P::READY_SLOTS,
        }
    }

    pub fn for_tcp_profile<P: DriverProfile>(max_connections: usize) -> Self {
        let max_connections = u32::try_from(max_connections).unwrap_or(u32::MAX);
        let accept_slots =
            max_connections.min(P::FIXED_FILE_SLOTS.saturating_sub(P::OUTBOUND_RESERVE));
        Self {
            ring_entries: Self::sized_sq(accept_slots, P::OUTBOUND_RESERVE).min(P::RING_ENTRIES),
            cq_entries: P::CQ_ENTRIES,
            fixed_file_slots: accept_slots.saturating_add(P::OUTBOUND_RESERVE),
            accept_slots,
            provided: ProvidedBufferConfig::for_accept(
                accept_slots,
                P::PROVIDED_BUF_LEN,
                Self::MAX_ENTRIES,
            ),
            defer_taskrun: P::DEFER_TASKRUN,
            ready_slots: P::READY_SLOTS,
        }
    }

    pub fn for_quic_udp(provided_buf_entries: u32, provided_buf_len: u32) -> Self {
        Self {
            ring_entries: 256,
            cq_entries: 1024,
            fixed_file_slots: 16,
            accept_slots: 0,
            provided: ProvidedBufferConfig {
                len: provided_buf_len as usize,
                entries: provided_buf_entries.min(u16::MAX as u32) as u16,
            },
            defer_taskrun: false,
            ready_slots: 1024,
        }
    }

    pub fn with_provided(mut self, len: usize, entries: u16) -> Self {
        self.provided.apply_overrides(len, entries);
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.accept_slots > self.fixed_file_slots {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "dope: accept slots exceed fixed-file capacity",
            ));
        }
        if TokenCapacity::new(self.fixed_file_slots as usize).is_none() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "dope: fixed-file capacity exceeds token slots",
            ));
        }
        Backend::snapshot()?
            .check_slots(self.fixed_file_slots)
            .map_err(io::Error::from)
    }
}

impl ProvidedBufferConfig {
    pub fn for_accept(accept_slots: u32, buf_len: usize, max_entries: u32) -> Self {
        const PROVIDED_FLOOR: u32 = 1024;
        const K_BATCH: u32 = 4;
        const DRAIN_BATCH: u32 = 256;

        let buf_len_ratio = (buf_len / 4096).max(1) as u32;
        let hwm_entries = K_BATCH
            .saturating_mul(DRAIN_BATCH)
            .min(accept_slots.max(PROVIDED_FLOOR));
        let target = hwm_entries.min(max_entries) / buf_len_ratio;
        Self {
            len: buf_len,
            entries: target.max(PROVIDED_FLOOR).min(u16::MAX as u32) as u16,
        }
    }

    pub fn apply_overrides(&mut self, len: usize, entries: u16) {
        if len != 0 {
            self.len = len;
        }
        if entries != 0 {
            self.entries = entries;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PushError;

impl Display for PushError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("dope: SQE push failed")
    }
}

impl Error for PushError {}

impl From<PushError> for io::Error {
    fn from(_: PushError) -> Self {
        io::Error::from(ErrorKind::WouldBlock)
    }
}

pub struct OutboundReservation<'d> {
    base: u32,
    capacity: u32,
    _brand: Invariant<'d>,
    _thread: ThreadBound,
}

#[derive(Clone, Copy)]
pub struct OutboundSlot<'d> {
    fd: FdSlot,
    _brand: Invariant<'d>,
}

impl<'d> OutboundSlot<'d> {
    pub fn bind(self, driver: DriverRef<'d>) -> Fd<'d> {
        Fd::from_range_slot(self.fd, driver)
    }
}

impl<'d> OutboundReservation<'d> {
    pub(crate) fn new(base: u32, capacity: u32) -> Self {
        Self {
            base,
            capacity,
            _brand: PhantomData,
            _thread: ThreadBound::NEW,
        }
    }

    pub fn empty() -> Self {
        Self {
            base: 0,
            capacity: 0,
            _brand: PhantomData,
            _thread: ThreadBound::NEW,
        }
    }

    pub fn slot(&self, local: SlotIndex) -> Option<OutboundSlot<'d>> {
        if local.raw() >= self.capacity {
            return None;
        }
        let index = SlotIndex::try_new(self.base.checked_add(local.raw())?)?;
        Some(OutboundSlot {
            fd: FdSlot::from_index(index),
            _brand: PhantomData,
        })
    }

    pub(crate) fn into_range(self) -> (u32, u32) {
        (self.base, self.capacity)
    }
}
