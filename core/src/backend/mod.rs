pub(crate) mod ops;

cfg_select! {
    target_os = "linux" => {
        pub mod uring;
        pub type Backend = uring::driver::Uring;
        pub use uring::sqe::{RawSqe, Sqe};
        pub use uring::platform::gso::{Gso, MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
        pub type StatBuf = libc::statx;
        pub type TimerSpec = io_uring::types::Timespec;
    }
    _ => {
        pub mod kqueue;
        pub type Backend = kqueue::driver::Kqueue;
        pub use kqueue::sqe::{RawSqe, Sqe};
        pub use kqueue::platform::gso::{Gso, MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
        pub type StatBuf = libc::stat;
        pub type TimerSpec = kqueue::sqe::TimerSpec;
    }
}
