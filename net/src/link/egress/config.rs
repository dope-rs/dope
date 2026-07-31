use super::{EGRESS_CAP_BYTES, EGRESS_CAP_ENTRIES};

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

    pub(super) fn entries(self) -> usize {
        self.entries as usize
    }

    pub(super) fn wire_bytes(self) -> u32 {
        self.bytes
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            reserved_entries: EGRESS_CAP_ENTRIES / 2,
            shared_entries: EGRESS_CAP_ENTRIES / 2,
            reserved_bytes: EGRESS_CAP_BYTES / 2,
            shared_bytes: EGRESS_CAP_BYTES / 2,
            entries: EGRESS_CAP_ENTRIES,
            bytes: EGRESS_CAP_BYTES,
        }
    }
}
