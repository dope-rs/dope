#![warn(unreachable_pub)]

extern crate self as dope;

pub mod hash;
pub mod manifold;
pub mod panic;
pub mod runtime;

pub use dope_core::driver::bootstrap::Bootstrap;
pub use dope_core::driver::buffers::ProvidedBuffers;
pub use dope_core::driver::completion::Completion;
pub use dope_core::driver::control::ContextControl;
pub use dope_core::driver::datagram::Datagram;
pub use dope_core::driver::ext::DriverExt;
pub use dope_core::driver::submission::Submission;
pub use dope_core::driver::{Driver, DriverContext, DriverRef, OutboundReservation, PushError};
pub use dope_core::io::provided::{ProvidedLease, ProvidedView};
pub use dope_core::io::{
    AcceptEvent, ConnectEvent, Cqe, DecodeError, Event, EventRef, OpenEvent, ReadEvent, RecvEvent,
    SendEvent, SocketEvent, SpliceEvent, SyncEvent, WriteEvent,
};
pub use dope_core::{driver, io, platform};
