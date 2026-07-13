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
    AcceptEvent, ConnectEvent, Cqe, Event, OpenEvent, ReadEvent, RecvEvent, SendEvent, SocketEvent,
    SpliceEvent, SyncEvent, WriteEvent,
};
pub(super) use datagram::Datagram;
pub use drive::Drive;
pub use lend::Lend;
pub use sockopt::Sockopt;

pub type DriverCfg = <Default as Backend>::Config;

/// Size the provided recv-buffer pool to the recv high-water-mark — a small
/// multiple of the CQE drain batch — NOT to the connection count. Shared by the
/// io_uring and kqueue backends so both paths stay flat in conn count.
///
/// Provided buffers are released within the same drain batch they are checked
/// out (proof in MEMORY_DESIGN.md section 0), so the held set never exceeds
/// [`DRAIN_BATCH`](crate::runtime::executor::DRAIN_BATCH) regardless of
/// `accept_slots`. `K_BATCH` standing batches leave the kernel several batches
/// of inventory between flushes. Larger recv buffers (e.g. Throughput's 64 KiB)
/// divide the entry count by their page ratio so total bytes stay bounded;
/// floored so tiny deployments keep slack. `max_entries` is the backend's
/// ring-buffer ceiling.
pub(crate) fn provided_entries(
    accept_slots: u32,
    provided_buf_len: usize,
    max_entries: u32,
) -> u16 {
    /// Standing-inventory floor: tiny deployments keep this much slack.
    const PROVIDED_FLOOR: u32 = 1024;
    /// Standing inventory = K_BATCH drained batches of recv buffers (4x the
    /// `<=DRAIN_BATCH` recv HWM). Raise if the `enobufs` counter is non-zero at
    /// sustained peak.
    const K_BATCH: u32 = 4;

    let drain_batch = crate::runtime::executor::DRAIN_BATCH as u32;
    let buf_len_ratio = (provided_buf_len / 4096).max(1) as u32;
    // Cap the standing inventory at the conn count: a deployment with fewer
    // conns than the batch HWM cannot have more recvs in flight than conns.
    let hwm_entries = K_BATCH
        .saturating_mul(drain_batch)
        .min(accept_slots.max(PROVIDED_FLOOR));
    let target = hwm_entries.min(max_entries) / buf_len_ratio;
    target.max(PROVIDED_FLOOR).min(u16::MAX as u32) as u16
}

/// Apply explicit provided-pool overrides in place; a zero field is left
/// unchanged. Centralizes the "0 = keep the profile-derived value" contract of
/// [`DriverConfig::with_provided`].
#[inline]
pub(crate) fn apply_provided(len: usize, entries: u16, dst_len: &mut usize, dst_entries: &mut u16) {
    if len != 0 {
        *dst_len = len;
    }
    if entries != 0 {
        *dst_entries = entries;
    }
}

pub trait DriverConfig: Sized + Copy {
    fn for_profile<P: crate::backend::profile::Profile>() -> Self;

    fn for_tcp_profile<P: crate::backend::profile::Profile>(max_conn: usize) -> Self;
    fn for_quic_udp(provided_buf_entries: u32, provided_buf_len: u32) -> Self;

    fn with_cpu_id(self, _cpu: Option<u16>) -> Self {
        self
    }

    /// Override the provided recv-buffer pool sizing without minting a new
    /// [`Profile`](crate::backend::profile::Profile). A zero argument leaves the
    /// corresponding field at the profile-derived value.
    fn with_provided(self, _len: usize, _entries: u16) -> Self {
        self
    }
}

pub trait Backend: Sized {
    type Driver: 'static + crate::backend::Bootstrap;
    type Config: DriverConfig;

    fn new_driver(cfg: Self::Config) -> std::io::Result<Self::Driver>;
    fn init_process(cfg: &Self::Config) -> std::io::Result<()>;
    fn init_thread(cpu_id: u16) -> std::io::Result<()>;

    fn allowed_cpus() -> Vec<u16> {
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        (0..n as u16).collect()
    }
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

/// Fixed-port datagram bind = QUIC server socket shared per-core; reuse the
/// addr/port so every worker can bind it. Ephemeral (port 0) is private.
pub(crate) fn datagram_opts(addr: &SocketAddr) -> ListenerOpts {
    let reuse = addr.port() != 0;
    ListenerOpts {
        reuse_addr: reuse,
        reuse_port: reuse,
        ..ListenerOpts::default()
    }
}

pub trait Bootstrap {
    fn bind_listener_slot(
        &self,
        addr: SocketAddr,
        backlog: i32,
        opts: &ListenerOpts,
    ) -> io::Result<(socket::Fd<'_>, SocketAddr)>;

    fn bind_datagram_slot(&self, addr: SocketAddr) -> io::Result<(socket::Fd<'_>, SocketAddr)>;
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
