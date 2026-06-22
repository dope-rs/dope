mod cqe;
mod drive;
mod lend;
mod os_fd;
mod pipe;
mod sockaddr;
mod sockopt;

pub mod profile;

pub mod datagram;
pub mod file;
pub mod park;
pub mod socket;
pub mod token;

pub use cqe::{
    AcceptEvent, ConnectEvent, Cqe, Event, RecvEvent, SendEvent, SocketEvent, SyncEvent, WriteEvent,
};
pub(super) use datagram::Datagram;
pub use drive::Drive;
pub use lend::Lend;
pub use sockopt::Sockopt;

pub type DriverCfg = <Default as Backend>::Config;

pub trait DriverConfig: Sized + Copy {
    fn for_profile<P: crate::backend::profile::Profile>() -> Self;

    fn for_tcp_profile<P: crate::backend::profile::Profile>(max_conn: usize) -> Self;
    fn for_quic_udp(provided_buf_entries: u32, provided_buf_len: u32) -> Self;

    fn with_cpu_id(self, _cpu: Option<u16>) -> Self {
        self
    }
}

pub trait Backend: Sized {
    type Driver: 'static + crate::backend::Bootstrap;
    type Config: DriverConfig;

    fn new_driver(cfg: Self::Config) -> std::io::Result<Self::Driver>;
    fn init_process(cfg: &Self::Config) -> std::io::Result<()>;
    fn init_thread(cpu_id: u16) -> std::io::Result<()>;
}

cfg_select! {
    target_os = "linux" => {
        pub(super) mod uring;
        pub(super) type Default = uring::Backend;
        pub use uring::sqe;
        pub use uring::platform;
    }
    _ => {
        pub(super) mod kqueue;
        pub(super) type Default = kqueue::Backend;
        pub use kqueue::sqe;
        pub use kqueue::platform;
    }
}

use std::io;
use std::net::SocketAddr;

#[derive(Debug, Clone, Copy)]
pub struct PushError;

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dope: SQE push failed")
    }
}

impl std::error::Error for PushError {}

impl From<PushError> for io::Error {
    fn from(_: PushError) -> Self {
        io::Error::from(io::ErrorKind::WouldBlock)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ListenerOpts {
    pub reuse_addr: bool,
    pub reuse_port: bool,
    pub fastopen_backlog: Option<u32>,
    pub defer_accept_secs: Option<u32>,
}

impl core::default::Default for ListenerOpts {
    fn default() -> Self {
        Self {
            reuse_addr: true,
            reuse_port: false,
            fastopen_backlog: None,
            defer_accept_secs: None,
        }
    }
}

pub trait Bootstrap {
    fn bind_listener_slot(
        &mut self,
        addr: SocketAddr,
        backlog: i32,
        opts: &ListenerOpts,
    ) -> io::Result<(socket::Fd, SocketAddr)>;

    fn bind_datagram_slot(&mut self, addr: SocketAddr) -> io::Result<(socket::Fd, SocketAddr)>;
}

pub struct OutboundReservation {
    base: u32,
    capacity: u32,
}

impl OutboundReservation {
    pub(super) fn new(base: u32, capacity: u32) -> Self {
        Self { base, capacity }
    }

    pub fn empty() -> Self {
        Self {
            base: 0,
            capacity: 0,
        }
    }

    pub fn absolute(&self, local: crate::backend::token::LocalIdx) -> socket::FdSlot {
        socket::FdSlot::new(self.base + local.raw())
    }

    pub fn try_absolute(&self, local: crate::backend::token::LocalIdx) -> Option<socket::FdSlot> {
        (local.raw() < self.capacity).then(|| socket::FdSlot::new(self.base + local.raw()))
    }
}
