#![warn(unreachable_pub)]

mod backend;
pub mod memstats;
mod slab;

pub mod fiber;
pub mod launcher;
pub mod manifold;
pub mod runtime;
pub mod transport;

pub use backend::park::WakeRef;
pub use backend::profile::Profile;
pub use backend::{
    AcceptEvent, Backend, Bootstrap, ConnectEvent, Cqe, Drive, DriverCfg, DriverConfig, Event,
    Lend, ListenerOpts, OpenEvent, OutboundReservation, PushError, ReadEvent, RecvEvent, SendEvent,
    SocketEvent, Sockopt, SpliceEvent, SyncEvent, WriteEvent, datagram, file, platform, socket,
    sqe, token,
};
pub use fiber::WakerSet;
pub use o3;
pub use runtime::dispatcher::{Dispatcher, Idle};
pub use runtime::executor::Executor;
pub use runtime::executor::Session;
pub use transport::wire;

pub type Driver = <backend::Default as Backend>::Driver;
