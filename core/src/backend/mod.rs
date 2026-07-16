cfg_select! {
    target_os = "linux" => {
        pub mod uring;
        pub type Backend = uring::driver::Uring;
        pub use uring::sqe::Sqe;
        pub use uring::platform::gso::{Gso, MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
        pub(crate) use uring::platform::abi::PlatformAbi;
    }
    _ => {
        pub mod kqueue;
        pub type Backend = kqueue::driver::Kqueue;
        pub use kqueue::sqe::Sqe;
        pub use kqueue::platform::gso::{Gso, MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
        pub(crate) use kqueue::platform::abi::PlatformAbi;
    }
}
