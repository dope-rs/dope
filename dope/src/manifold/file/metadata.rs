use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dope_core::io::file::RawMetadata;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Metadata {
    len: u64,
    modified: Option<SystemTime>,
    file: bool,
}

impl Metadata {
    pub fn len(self) -> u64 {
        self.len
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn modified(self) -> Option<SystemTime> {
        self.modified
    }

    pub fn is_file(self) -> bool {
        self.file
    }

    pub(crate) fn from_raw(meta: RawMetadata) -> Self {
        Self {
            len: meta.len,
            modified: meta
                .modified
                .and_then(|(seconds, nanos)| Self::modified_time(seconds, nanos)),
            file: meta.regular,
        }
    }

    fn modified_time(seconds: i64, nanos: u32) -> Option<SystemTime> {
        if seconds >= 0 {
            UNIX_EPOCH.checked_add(Duration::new(seconds as u64, nanos))
        } else {
            UNIX_EPOCH
                .checked_sub(Duration::from_secs(seconds.unsigned_abs()))
                .and_then(|time| time.checked_add(Duration::from_nanos(u64::from(nanos))))
        }
    }
}
