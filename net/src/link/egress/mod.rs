pub mod data;
pub mod metadata;
mod queue;
pub mod raw;
mod storage;
mod write;

pub use queue::Queue;
pub(in crate::link) use queue::lanes::Lane;
pub(in crate::link) use storage::Storage;
pub use write::Write;

/// Progress made while retiring one egress lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
#[repr(u8)]
pub enum ClearProgress {
    /// The lane is empty and may be reused.
    Done,
    /// Retained entries remain, but this turn admitted no more cleanup work.
    Retry,
    /// A kernel flight still retains the lane.
    Waiting,
}

const CAP_BYTES: u32 = 1 << 20;
const CAP_ENTRIES: u32 = 1 << 12;

const _: () = assert!(usize::BITS >= u32::BITS);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub(super) reserved_entries: u32,
    pub(super) shared_entries: u32,
    pub(super) reserved_bytes: u32,
    pub(super) shared_bytes: u32,
    entries: u32,
    bytes: u32,
}

impl Config {
    pub const DEFAULT: Self = Self {
        reserved_entries: CAP_ENTRIES / 2,
        shared_entries: CAP_ENTRIES / 2,
        reserved_bytes: CAP_BYTES / 2,
        shared_bytes: CAP_BYTES / 2,
        entries: CAP_ENTRIES,
        bytes: CAP_BYTES,
    };
    pub const fn shared(entries: u32, bytes: u32) -> Self {
        Self {
            reserved_entries: 0,
            shared_entries: entries,
            reserved_bytes: 0,
            shared_bytes: bytes,
            entries,
            bytes,
        }
    }

    pub const fn partitioned(
        reserved_entries: u32,
        shared_entries: u32,
        reserved_bytes: u32,
        shared_bytes: u32,
    ) -> Option<Self> {
        match (
            reserved_entries.checked_add(shared_entries),
            reserved_bytes.checked_add(shared_bytes),
        ) {
            (Some(entries), Some(bytes)) => Some(Self {
                reserved_entries,
                shared_entries,
                reserved_bytes,
                shared_bytes,
                entries,
                bytes,
            }),
            _ => None,
        }
    }

    /// Maximum retained metadata entries owned by one arena.
    pub const fn entry_capacity(self) -> u32 {
        self.entries
    }

    /// Maximum resident payload bytes owned by one arena.
    pub const fn resident_capacity(self) -> u32 {
        self.bytes
    }

    pub(super) fn flight_capacity(self, lanes: usize) -> u32 {
        let Ok(lanes) = u32::try_from(lanes) else {
            return self.entries;
        };
        lanes.min(self.entries)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::DEFAULT
    }
}
