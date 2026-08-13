use std::time;

use dope_core::io::fs;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Metadata {
    len: u64,
    modified: Option<time::SystemTime>,
}

impl Metadata {
    pub const fn len(self) -> u64 {
        self.len
    }

    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn modified(self) -> Option<time::SystemTime> {
        self.modified
    }

    pub(crate) fn from_raw(meta: fs::RawMetadata) -> Self {
        Self {
            len: meta.len,
            modified: meta
                .modified
                .and_then(|(seconds, nanos)| Self::modified_time(seconds, nanos)),
        }
    }

    fn modified_time(seconds: i64, nanos: u32) -> Option<time::SystemTime> {
        use std::time::{Duration, UNIX_EPOCH};
        if seconds >= 0 {
            UNIX_EPOCH.checked_add(Duration::new(seconds as u64, nanos))
        } else {
            UNIX_EPOCH
                .checked_sub(Duration::from_secs(seconds.unsigned_abs()))
                .and_then(|time| time.checked_add(Duration::from_nanos(u64::from(nanos))))
        }
    }
}
