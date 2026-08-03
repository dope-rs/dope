pub(crate) mod raw;
pub mod snapshot;

use std::io;

use snapshot::Snapshot;

use crate::io::file::RawMetadata;

pub trait Platform {
    type Sqe;
    type Gso;
    type StatBuf;
    type TimerSpec;

    fn entropy() -> io::Result<[u64; 2]>;
    fn parse_meta(raw: &Self::StatBuf) -> io::Result<RawMetadata>;
    fn snapshot() -> io::Result<Snapshot>;
}
