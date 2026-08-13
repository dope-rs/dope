use std::{num, process};

use crate::{
    driver::route::table,
    io::{datagram, socket::option, transfer},
};

mod config;

pub use config::Config;

const MIN_SUBMISSION_CAPACITY: u32 = (option::MAX_STREAM_OPTIONS + 1) as u32;
const MAX_SUBMISSION_CAPACITY: u32 = 32_768;
const MAX_COMPLETION_CAPACITY: u32 = 65_536;

/// Backend-neutral intent for making completed work visible to the driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CompletionProgress {
    /// Avoid voluntarily deferring backend completion work.
    Prompt,
    /// Permit a backend to coalesce completion work until the next driver entry.
    BatchedWhenSupported,
}

const _: () = {
    assert!(std::mem::size_of::<CompletionProgress>() == std::mem::size_of::<bool>());
    assert!(std::mem::align_of::<CompletionProgress>() == std::mem::align_of::<bool>());
};

/// Static driver policy with a validated receive-buffer layout.
///
/// Invalid layouts cannot become profile constants:
///
/// ```compile_fail
/// use dope_core::driver::settings::{Profile, QueueLayout, Receive};
///
/// struct Invalid;
/// impl Profile for Invalid {
///     const QUEUES: QueueLayout = QueueLayout::fixed::<64, 128>();
///     const RECEIVE: Receive = Receive::fixed::<3, 4096>();
/// }
///
/// let _ = Invalid::RECEIVE;
/// ```
pub trait Profile: 'static {
    const QUEUES: QueueLayout;
    const SCHEDULER: SchedulerLayout = SchedulerLayout::DEFAULT;
    const COMPLETION_PROGRESS: CompletionProgress = CompletionProgress::BatchedWhenSupported;

    /// Maximum listener-owned accept slots realized by `for_tcp_profile`.
    const MAX_ACCEPT_SLOTS: u32 = 65_279;
    /// Descriptor slots available to outbound pools and standalone files.
    const OUTBOUND_SLOTS: u32 = 256;
    const RECEIVE: Receive = Receive::DEFAULT;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
struct SubmissionCapacity(u32);

impl SubmissionCapacity {
    const fn new(entries: u32) -> Option<Self> {
        if entries == 0 {
            return None;
        }
        if entries < MIN_SUBMISSION_CAPACITY
            || !entries.is_power_of_two()
            || entries > MAX_SUBMISSION_CAPACITY
        {
            return None;
        }
        Some(Self(entries))
    }

    const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueLayout {
    submissions: SubmissionCapacity,
    completions: u32,
}

struct FixedQueueLayout<const SUBMISSIONS: u32, const COMPLETIONS: u32>;

impl<const SUBMISSIONS: u32, const COMPLETIONS: u32> FixedQueueLayout<SUBMISSIONS, COMPLETIONS> {
    const VALUE: QueueLayout = {
        assert!(SUBMISSIONS >= MIN_SUBMISSION_CAPACITY);
        assert!(SUBMISSIONS.is_power_of_two());
        assert!(SUBMISSIONS <= MAX_SUBMISSION_CAPACITY);
        assert!(COMPLETIONS.is_power_of_two());
        assert!(COMPLETIONS <= MAX_COMPLETION_CAPACITY);
        assert!(COMPLETIONS >= SUBMISSIONS);
        QueueLayout {
            submissions: SubmissionCapacity(SUBMISSIONS),
            completions: COMPLETIONS,
        }
    };
}

impl QueueLayout {
    pub const MAX: Self = Self::fixed::<MAX_SUBMISSION_CAPACITY, MAX_COMPLETION_CAPACITY>();

    /// Creates a queue layout validated during compilation.
    ///
    /// ```compile_fail
    /// use dope_core::driver::settings::QueueLayout;
    ///
    /// let _ = QueueLayout::fixed::<64, 32>();
    /// ```
    pub const fn fixed<const SUBMISSIONS: u32, const COMPLETIONS: u32>() -> Self {
        FixedQueueLayout::<SUBMISSIONS, COMPLETIONS>::VALUE
    }

    /// Creates power-of-two backend queues. Submissions cover atomic stream
    /// setup and cap at 32,768; completions cover submissions and cap at 65,536.
    #[must_use]
    pub const fn new(submissions: u32, completions: u32) -> Option<Self> {
        let Some(submissions) = SubmissionCapacity::new(submissions) else {
            return None;
        };
        if completions == 0 {
            return None;
        }
        if !completions.is_power_of_two()
            || completions > MAX_COMPLETION_CAPACITY
            || completions < submissions.get()
        {
            return None;
        }
        Some(Self {
            submissions,
            completions,
        })
    }

    #[must_use]
    pub const fn submissions(self) -> u32 {
        self.submissions.get()
    }

    #[must_use]
    pub const fn completions(self) -> u32 {
        self.completions
    }
}

const _: () = {
    assert!(QueueLayout::new(0, 8).is_none());
    assert!(QueueLayout::new(MIN_SUBMISSION_CAPACITY / 2, 8).is_none());
    assert!(QueueLayout::new(64, 32).is_none());
    assert!(QueueLayout::MAX.submissions() == MAX_SUBMISSION_CAPACITY);
    assert!(QueueLayout::MAX.completions() == MAX_COMPLETION_CAPACITY);
    assert!(std::mem::size_of::<QueueLayout>() == std::mem::size_of::<[u32; 2]>());
};

/// Scheduler-owned index capacity matching ready and timer slot indices.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct ScheduleCapacity(u32);

impl ScheduleCapacity {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u32::MAX);

    /// Creates a scheduler capacity represented directly by its internal index.
    pub const fn fixed<const SLOTS: u32>() -> Self {
        Self(SLOTS)
    }

    #[must_use]
    pub const fn new(slots: usize) -> Option<Self> {
        if slots <= u32::MAX as usize {
            Some(Self(slots as u32))
        } else {
            None
        }
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0 as usize
    }
}

/// Independent ready-slot and timer-cache bounds. Timer registrations beyond
/// the realized cache remain live in the intrusive overflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulerLayout {
    ready: ScheduleCapacity,
    timer_cache_limit: ScheduleCapacity,
}

impl SchedulerLayout {
    pub const DEFAULT: Self = Self::fixed::<1024, 1024>();

    pub const fn fixed<const READY: u32, const TIMER_CACHE: u32>() -> Self {
        Self {
            ready: ScheduleCapacity::fixed::<READY>(),
            timer_cache_limit: ScheduleCapacity::fixed::<TIMER_CACHE>(),
        }
    }

    #[must_use]
    pub const fn new(ready: usize, timer_cache_limit: usize) -> Option<Self> {
        let Some(ready) = ScheduleCapacity::new(ready) else {
            return None;
        };
        let Some(timer_cache_limit) = ScheduleCapacity::new(timer_cache_limit) else {
            return None;
        };
        Some(Self {
            ready,
            timer_cache_limit,
        })
    }

    #[must_use]
    pub const fn ready(self) -> ScheduleCapacity {
        self.ready
    }

    #[must_use]
    pub const fn timer_cache_limit(self) -> ScheduleCapacity {
        self.timer_cache_limit
    }

    #[must_use]
    pub const fn with_ready(mut self, ready: ScheduleCapacity) -> Self {
        self.ready = ready;
        self
    }

    #[must_use]
    pub const fn with_timer_cache_limit(mut self, timer_cache_limit: ScheduleCapacity) -> Self {
        self.timer_cache_limit = timer_cache_limit;
        self
    }
}

const _: () = {
    assert!(ScheduleCapacity::new(0).is_some());
    assert!(ScheduleCapacity::MAX.get() == u32::MAX as usize);
    if let Some(overflow) = (u32::MAX as usize).checked_add(1) {
        assert!(ScheduleCapacity::new(overflow).is_none());
    }
    assert!(std::mem::size_of::<ScheduleCapacity>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<SchedulerLayout>() == std::mem::size_of::<[u32; 2]>());
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receive {
    entries: u16,
    len: u32,
}

struct FixedReceive<const ENTRIES: u16, const LEN: u32>;

impl<const ENTRIES: u16, const LEN: u32> FixedReceive<ENTRIES, LEN> {
    const VALUE: Receive = {
        assert!(ENTRIES >= MIN_RECEIVE_ENTRIES);
        assert!(ENTRIES.is_power_of_two());
        assert!(LEN != 0);
        assert!(LEN <= transfer::MAX_BYTES as u32);
        assert!((ENTRIES as usize) <= (isize::MAX as usize) / (LEN as usize));
        Receive {
            entries: ENTRIES,
            len: LEN,
        }
    };
}

const MIN_RECEIVE_ENTRIES: u16 = 2;

const _: () = {
    assert!(std::mem::size_of::<Receive>() == std::mem::size_of::<u64>());
};

impl Receive {
    pub const DEFAULT: Self = Self::fixed::<4096, 4096>();
    pub const MIN_DATAGRAM_BUFFER_LEN: u32 = datagram::SlotLen::MIN_BYTES;

    /// Creates a receive-buffer layout validated during compilation.
    ///
    /// ```compile_fail
    /// use dope_core::driver::settings::Receive;
    ///
    /// let _ = Receive::fixed::<1, 4096>();
    /// ```
    pub const fn fixed<const ENTRIES: u16, const LEN: u32>() -> Self {
        FixedReceive::<ENTRIES, LEN>::VALUE
    }

    /// Creates a cross-backend layout with at least two power-of-two slots.
    /// Slot length must fit transfer fields and total allocation in `isize`.
    #[must_use]
    pub const fn new(entries: u16, len: u32) -> Option<Self> {
        if len == 0 {
            return None;
        }
        if len > transfer::MAX_BYTES as u32 {
            return None;
        }
        Self::with_len(entries, len)
    }

    /// Creates a layout with the requested cross-backend datagram payload capacity.
    #[must_use]
    pub const fn for_datagram_payload(entries: u16, payload_len: u32) -> Option<Self> {
        let Some(len) = datagram::SlotLen::for_payload(payload_len) else {
            return None;
        };
        Self::with_len(entries, len.nonzero().get())
    }

    const fn with_len(entries: u16, len: u32) -> Option<Self> {
        if entries < MIN_RECEIVE_ENTRIES || !entries.is_power_of_two() {
            return None;
        }
        let Some(bytes) = (entries as usize).checked_mul(len as usize) else {
            return None;
        };
        if bytes > isize::MAX as usize {
            return None;
        }
        Some(Self { entries, len })
    }

    #[must_use]
    pub const fn entries(self) -> u16 {
        self.entries
    }

    #[must_use]
    pub const fn buffer_len(self) -> usize {
        self.len as usize
    }

    pub(crate) fn nonzero_buffer_len(self) -> num::NonZeroU32 {
        let Some(len) = num::NonZeroU32::new(self.len) else {
            process::abort();
        };
        len
    }

    pub(crate) const fn backing_bytes(self) -> usize {
        self.entries as usize * self.len as usize
    }
}

/// A fixed-file layout representable by every backend target table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileSlots {
    accept_end: table::Capacity,
    table_end: table::Capacity,
}

struct FixedFileSlots<const ACCEPT: u32, const OUTBOUND: u32>;

impl<const ACCEPT: u32, const OUTBOUND: u32> FixedFileSlots<ACCEPT, OUTBOUND> {
    const VALUE: FileSlots = {
        assert!(ACCEPT <= u32::MAX - OUTBOUND);
        let total = ACCEPT + OUTBOUND;
        assert!(total != 0);
        FileSlots {
            accept_end: table::Capacity::fixed(ACCEPT),
            table_end: table::Capacity::fixed(total),
        }
    };
}

const _: () =
    assert!(std::mem::size_of::<FileSlots>() == std::mem::size_of::<[table::Capacity; 2]>());

impl FileSlots {
    /// Creates a fixed-file layout validated during compilation.
    ///
    /// ```compile_fail
    /// use dope_core::driver::settings::FileSlots;
    ///
    /// let _ = FileSlots::fixed::<16, { u32::MAX }>();
    /// ```
    pub const fn fixed<const ACCEPT: u32, const OUTBOUND: u32>() -> Self {
        FixedFileSlots::<ACCEPT, OUTBOUND>::VALUE
    }

    /// Creates independent accept and outbound domains in one fixed-file table.
    #[must_use]
    pub const fn new(accept: u32, outbound: u32) -> Option<Self> {
        let Some(total) = accept.checked_add(outbound) else {
            return None;
        };
        if total == 0 {
            return None;
        }
        let Some(accept_end) = table::Capacity::new(accept as usize) else {
            return None;
        };
        let Some(table_end) = table::Capacity::new(total as usize) else {
            return None;
        };
        Some(Self {
            accept_end,
            table_end,
        })
    }

    #[must_use]
    pub const fn capacity(self) -> u32 {
        self.table_end.raw()
    }

    #[must_use]
    pub const fn accept(self) -> u32 {
        self.accept_end.raw()
    }

    #[must_use]
    pub const fn outbound(self) -> u32 {
        self.table_end.raw() - self.accept_end.raw()
    }

    pub(crate) const fn table_capacity(self) -> table::Capacity {
        self.table_end
    }

    pub(crate) const fn accept_capacity(self) -> table::Capacity {
        self.accept_end
    }
}
