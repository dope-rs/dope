mod fixed;
pub(crate) mod ops;

cfg_select! {
    target_os = "linux" => {
        pub mod uring;
        pub type Backend = uring::driver::Uring;
        pub(crate) type RecvBuffer = uring::provided::ffi::ring::Buffer;
        pub use uring::sqe::{RawSqe, Sqe};
        pub use uring::platform::gso::{Gso, MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
        pub type StatBuf = libc::statx;
        pub type TimerSpec = io_uring::types::Timespec;
    }
    _ => {
        pub mod kqueue;
        pub type Backend = kqueue::driver::Kqueue;
        pub(crate) type RecvBuffer = kqueue::recv_pool::ffi::pool::Buffer;
        pub use kqueue::sqe::{RawSqe, Sqe};
        pub use kqueue::platform::gso::{Gso, MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
        pub type StatBuf = libc::stat;
        pub type TimerSpec = kqueue::sqe::TimerSpec;
    }
}

/// A raw submission whose retained resources have been proven stable.
#[repr(transparent)]
pub struct RetainedSqe(RawSqe);

/// An owner-backed source for one retained kernel submission.
/// # Safety
/// Captured resources remain valid and correctly aliased through terminal completion or quiescence.
#[doc(hidden)]
pub unsafe trait StableSqeSource {
    fn into_raw(self) -> RawSqe;
}

impl RetainedSqe {
    #[doc(hidden)]
    pub fn from_stable(source: impl StableSqeSource) -> Self {
        Self(source.into_raw())
    }
}
