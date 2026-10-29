use std::num;

pub use config::Config;
pub(crate) use config::plan::{Plan, Ready};
use demands::slots;
use dope_sys::io::reactor::complete;
use o3::{base::num::bounded, depot, depot::capacity};
pub use receive::Receive;
pub use resource::Resource;
pub use routes::Routes;

use crate::{
    driver::route::{SLOT_MASK, indices::SlotIndex, table},
    io::{datagram::payload, socket::option, transfer},
};

pub mod claim;
mod config;
pub mod demands;
pub mod errors;
mod receive;
mod resource;
mod routes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DomainSum {
    current: Domain,
    added: Domain,
}

impl DomainSum {
    pub const fn parts(&self) -> (&Domain, &Domain) {
        (&self.current, &self.added)
    }
}

/// Power-of-two receive slots valid for one datagram pool.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Ring(Buffers);

impl Ring {
    pub fn try_round(entries: u32) -> Result<Self, errors::Claim> {
        if entries < 2 {
            return Err(errors::Config::ReceiveEntriesOutOfRange {
                actual: entries,
                minimum: 2,
                maximum: u16::MAX as u32,
            }
            .into());
        }
        let count = entries
            .checked_next_power_of_two()
            .ok_or(errors::Claim::ReceiveEntriesPowerOfTwoOverflow { requested: entries })?;
        if count > u32::from(u16::MAX) {
            return Err(errors::Claim::ReceiveEntriesTooLarge {
                actual: count as usize,
                maximum: u16::MAX as usize,
            });
        }
        Ok(Self(Buffers(
            num::NonZero::<u16>::MIN.saturating_add(count as u16 - 1),
        )))
    }

    pub const fn buffers(self) -> Buffers {
        self.0
    }
}

impl TryFrom<u16> for Ring {
    type Error = errors::Config;

    fn try_from(entries: u16) -> Result<Self, Self::Error> {
        if entries < 2 {
            return Err(errors::Config::ReceiveEntriesOutOfRange {
                actual: entries as u32,
                minimum: 2,
                maximum: u16::MAX as u32,
            });
        }
        if !entries.is_power_of_two() {
            return Err(errors::Config::ReceiveEntriesNotPowerOfTwo { actual: entries });
        }
        Ok(Self(Buffers(
            num::NonZero::<u16>::MIN.saturating_add(entries - 1),
        )))
    }
}

const _: () = assert!(std::mem::size_of::<Ring>() == std::mem::size_of::<Buffers>());

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Datagram(Receive);

impl Datagram {
    /// Constructs a datagram layout whose bounds are checked during compilation.
    ///
    pub const fn fixed<const ENTRIES: u16, const PAYLOAD: u32>() -> Self {
        const {
            assert!(ENTRIES >= 2);
            assert!(ENTRIES.is_power_of_two());
            let len = transfer::NonZero::from_payload(payload::Len::fixed::<PAYLOAD>());
            assert!((ENTRIES as u64) * (len.into_nonzero().get() as u64) <= isize::MAX as u64);
            let entries = Buffers(num::NonZero::<u16>::MIN.saturating_add(ENTRIES - 1));
            Self(Receive::from_parts(entries, len))
        }
    }

    pub fn try_new(entries: Ring, payload: payload::Len) -> Result<Self, errors::Config> {
        let len = transfer::NonZero::from_payload(payload);
        Ok(Self(Receive::assemble(entries, len)?))
    }

    pub const fn receive(self) -> Receive {
        self.0
    }
}

const _: () = {
    assert!(std::mem::size_of::<Datagram>() == std::mem::size_of::<Receive>());
    assert!(std::mem::align_of::<Datagram>() == std::mem::align_of::<Receive>());
};

const MIN_SUBMISSION_CAPACITY: u32 = (option::MAX_STREAM_OPTIONS + 1) as u32;
const MAX_SUBMISSION_CAPACITY: u32 = 32_768;
const MAX_COMPLETION_CAPACITY: u32 = 65_536;
pub(crate) const MAX_FILE_CAPACITY: u32 = SLOT_MASK as u32 + 1;

/// Backend-neutral intent for making completed work visible to the driver.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Progress {
    /// Avoid voluntarily deferring backend completion work.
    Prompt,
    /// Permit a backend to coalesce completion work until the next driver entry.
    BatchedWhenSupported,
}

/// Static driver policy with a validated receive-buffer layout.
///
/// Invalid layouts cannot become profile constants:
///
pub trait Profile {
    const QUEUES: QueueLayout;
    const SCHEDULER: Schedule = Schedule::DEFAULT;
    const COMPLETION_PROGRESS: Progress = Progress::BatchedWhenSupported;
    /// Backend descriptor baseline. Topology-owned accept and outbound demand
    /// is composed separately through [`Resource`].
    const FILES: FileSlots = FileSlots::fixed::<0, 256>();
    const RECEIVE: Receive = Receive::DEFAULT;
    /// Stable resident-accounting domains available to one driver scope.
    const RESIDENT_DOMAINS: Domain = Domain::ZERO;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Buffers(num::NonZero<u16>);

impl Buffers {
    pub const fn into_nonzero(self) -> num::NonZero<u16> {
        self.0
    }

    pub(crate) const fn get(self) -> u16 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueLayout {
    submissions: Submission,
    completions: Completion,
}

impl QueueLayout {
    /// Creates a queue layout validated during compilation.
    pub const fn fixed<const SUBMISSIONS: u32, const COMPLETIONS: u32>() -> Self {
        const {
            assert!(SUBMISSIONS >= MIN_SUBMISSION_CAPACITY);
            assert!(SUBMISSIONS <= MAX_SUBMISSION_CAPACITY);
            assert!(SUBMISSIONS.is_power_of_two());
            assert!(COMPLETIONS != 0);
            assert!(COMPLETIONS <= MAX_COMPLETION_CAPACITY);
            assert!(COMPLETIONS.is_power_of_two());
            assert!(COMPLETIONS >= SUBMISSIONS);
            QueueLayout {
                submissions: Submission(bounded::U32::clamp(SUBMISSIONS as usize)),
                completions: Completion(bounded::U32::clamp(COMPLETIONS as usize)),
            }
        }
    }

    /// Enlarges submission capacity and keeps the completion queue at least as large.
    pub fn resolve(self, need: demands::Submit) -> Result<Self, errors::Claim> {
        let wanted = self.submissions().max(need.get());
        let submit = wanted
            .checked_next_power_of_two()
            .ok_or(errors::Claim::QueueSubmissionsPowerOfTwoOverflow { requested: wanted })?;
        if submit > MAX_SUBMISSION_CAPACITY {
            return Err(errors::Config::QueueSubmissionsOutOfRange {
                actual: submit,
                minimum: MIN_SUBMISSION_CAPACITY,
                maximum: MAX_SUBMISSION_CAPACITY,
            }
            .into());
        }

        let complete = self.completions().max(submit);

        Ok(Self {
            submissions: Submission(bounded::U32::clamp(submit as usize)),
            completions: Completion(bounded::U32::clamp(complete as usize)),
        })
    }

    pub const fn submission(self) -> Submission {
        self.submissions
    }

    pub(crate) const fn completion(self) -> Completion {
        self.completions
    }

    pub(crate) const fn submissions(self) -> u32 {
        self.submissions.get()
    }

    pub(crate) const fn completions(self) -> u32 {
        self.completions.get()
    }
}

/// Independent bounds for ready work and live timer registrations.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    ready: slots::Slots,
    timers: slots::Slots,
}

impl Schedule {
    pub const DEFAULT: Self = Self::fixed::<1024, 1024>();

    pub const fn fixed<const READY: u32, const TIMERS: u32>() -> Self {
        Self {
            ready: slots::Slots::fixed::<READY>(),
            timers: slots::Slots::fixed::<TIMERS>(),
        }
    }

    #[must_use]
    pub const fn resolve(self, need: demands::Schedule) -> Self {
        Self {
            ready: self.ready.max(need.ready()),
            timers: self.timers.max(need.timers()),
        }
    }

    #[must_use]
    pub const fn ready(self) -> slots::Slots {
        self.ready
    }

    #[must_use]
    pub const fn timers(self) -> slots::Slots {
        self.timers
    }
}

/// A descriptor-slot layout representable by every backend target table.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FileSlots {
    accept: table::Capacity,
    table: bounded::U32<1, MAX_FILE_CAPACITY>,
}

impl FileSlots {
    /// Creates a descriptor-slot layout validated during compilation.
    ///
    ///
    pub const fn fixed<const ACCEPT: u32, const OUTBOUND: u32>() -> Self {
        const {
            assert!(ACCEPT <= u32::MAX - OUTBOUND);
            assert!(OUTBOUND != 0);
            let total = ACCEPT + OUTBOUND;
            assert!(total <= MAX_FILE_CAPACITY);
            FileSlots {
                accept: table::Capacity(capacity::Count::new(ACCEPT)),
                table: bounded::U32::clamp(total as usize),
            }
        }
    }

    /// Resolves descriptor demand against this validated baseline.
    pub fn resolve(self, need: demands::Files) -> Result<Self, errors::Config> {
        let accept = self.accept().raw().max(need.accepted().get());
        let outbound = self.outbound().raw().max(need.outbound().get());
        let total = accept as u64 + outbound as u64;
        let maximum = MAX_FILE_CAPACITY;
        if total > maximum as u64 {
            return Err(errors::Config::DescriptorCapacityOutOfRange {
                accepted: accept,
                outbound,
                actual: total,
                minimum: 1,
                maximum,
            });
        }
        Ok(Self {
            accept: table::Capacity(capacity::Count::new(accept)),
            table: bounded::U32::clamp(total as usize),
        })
    }

    #[must_use]
    pub const fn capacity(self) -> table::Capacity {
        table::Capacity(capacity::Count::new(self.table.get()))
    }

    #[must_use]
    pub const fn accept(self) -> table::Capacity {
        self.accept
    }

    #[must_use]
    pub const fn outbound(self) -> table::Capacity {
        table::Capacity(capacity::Count::new(self.table.get() - self.accept.raw()))
    }

    pub(crate) fn slots(self) -> impl DoubleEndedIterator<Item = SlotIndex> + ExactSizeIterator {
        self.capacity()
            .into_iter()
            .rev()
            .take(self.outbound().count().get())
            .rev()
    }

    pub(crate) fn accept_slot(self) -> Option<SlotIndex> {
        let span = self.accept.span()?;
        Some(SlotIndex::from_field(u64::from(span.get())))
    }

    pub(crate) const fn bound(self) -> bounded::U32<1, MAX_FILE_CAPACITY> {
        self.table
    }
}

/// Representable number of resident-accounting domains in one driver scope.
///
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Domain(u32);

impl Domain {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(1);
    pub const MAX: Self = Self(u32::MAX);

    /// Creates a count across the complete `u32` representation.
    pub const fn fixed<const COUNT: u32>() -> Self {
        Self(COUNT)
    }

    /// Resolves additive resident-domain demand against this baseline.
    #[must_use]
    pub const fn resolve(self, need: Self) -> Self {
        if self.0 >= need.0 { self } else { need }
    }

    pub const fn combine(self, next: Self) -> Result<Self, DomainSum> {
        match self.0.checked_add(next.0) {
            Some(total) => Ok(Self(total)),
            None => Err(DomainSum {
                current: self,
                added: next,
            }),
        }
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

const _: () = {
    assert!(std::mem::size_of::<Domain>() == std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<Domain>() == std::mem::align_of::<u32>());
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Submission(bounded::U32<MIN_SUBMISSION_CAPACITY, MAX_SUBMISSION_CAPACITY>);

impl Submission {
    pub(crate) const fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub(crate) struct Completion(pub(crate) bounded::U32<1, MAX_COMPLETION_CAPACITY>);

impl Completion {
    pub(crate) const fn get(self) -> u32 {
        self.0.get()
    }
}

const _: () = {
    assert!(std::mem::size_of::<Completion>() == std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<Completion>() == std::mem::align_of::<u32>());
};

impl From<FileSlots> for (table::Capacity, table::Capacity) {
    fn from(layout: FileSlots) -> Self {
        (layout.capacity(), layout.accept)
    }
}

// SAFETY: Completion retains one immutable bounded queue capacity in every copy.
unsafe impl depot::raw::Count for Completion {
    const MAX: u32 = MAX_COMPLETION_CAPACITY;

    fn count(self) -> capacity::Count {
        capacity::Count::new(self.0.get())
    }
}

// SAFETY: every copy retains the same immutable bounded capacity.
unsafe impl complete::raw::Capacity for Completion {}
