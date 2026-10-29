use std::{iter, mem};

use crate::{Errno, kernel::errno};

pub mod signal;
pub mod timer;

pub mod affinity;
mod attrs;
mod handle;
pub mod raw;

pub use attrs::Attrs;
pub use handle::Handle;

/// Smallest stack extent the C library accepts for one spawned thread.
pub const STACK_MIN: usize = libc::PTHREAD_STACK_MIN;

/// Native thread services and their backend-specific signal and timer owners.
pub trait Thread: errno::Source {
    type Cpus: DoubleEndedIterator<Item = affinity::raw::Cpu>
        + ExactSizeIterator
        + iter::FusedIterator;
    type Timer: crate::Timer;
    type Receiver: signal::raw::Receiver;

    fn current() -> Result<Self::Cpus, affinity::Error>;
    fn bind(cpu: affinity::raw::Cpu) -> Result<(), affinity::Error>;

    /// Returns whether the caller is the process's original thread.
    fn is_main() -> bool;

    /// Admits the native IEEE CRC32 backend without shared feature state.
    /// A denied native query retains its exact errno; callers may use software CRC.
    fn try_crc32() -> Result<bool, Errno> {
        raw::try_crc32(Self::read)
    }

    /// Initializes N caller-owned bytes from the operating system; N must not exceed 256.
    fn entropy<const N: usize>(
        bytes: &mut mem::MaybeUninit<[u8; N]>,
    ) -> Result<&mut [u8; N], Errno> {
        raw::entropy::<Self, N>(bytes)
    }
}
