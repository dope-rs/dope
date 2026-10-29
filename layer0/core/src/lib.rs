//! Ownership and lifetime contracts.

pub mod driver;
pub mod io;
pub mod platform;
pub mod random;

pub use dope_sys::{self, Errno};
use dope_sys::{
    engine::{Removal, Source, Storage},
    table::permit,
};
use driver::{
    life::native::{create, host::Host},
    receive,
    result::{IoOperation, Kernel},
    schedule::ready::Stored,
};
pub use o3;

type Engine = <Backend as Source<Host<Backend>>>::Backend;
type Events = <Backend as Source<Host<Backend>>>::Map;
type Resources =
    <Backend as Storage<Host<Backend>, create::Failure, (IoOperation, Kernel)>>::Cleanup;
type Released =
    <Backend as Storage<Host<Backend>, create::Failure, (IoOperation, Kernel)>>::Release;
type RemoveSlot = <Backend as Removal>::Slot;
type Permits = <Backend as permit::Source>::Queue<Stored, receive::Index>;

/// Failure from the first or second branch of a statically composed operation.
#[derive(Debug)]
pub enum BranchError<A, B> {
    /// The first branch failed.
    First(A),
    /// The second branch failed.
    Second(B),
}

cfg_select! {
    target_os = "linux" => {
        pub type Backend = dope_uring::Uring;
        mod ring;
        use ring as completion;
    }
    target_os = "macos" => {
        pub type Backend = dope_kqueue::Kqueue;
        mod poll;
        use poll as completion;
    }
}
