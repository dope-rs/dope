pub mod link;
pub mod multishot;
mod option;
pub mod tcp;
pub mod unix;
pub mod wire;

pub use o3::buffer::{Bytes, Leased, RetainBytes};

use std::io;
use std::net::SocketAddr;

use dope_core::driver::DriverContext;
use dope_core::io::fd::Fd;
use dope_core::io::socket::addr::Addr;
use std::time::Duration;

pub trait Transport: 'static + Sized {
    type Addr;
    type StreamConfig: Default + Copy + 'static;

    const KERNEL_DISCARD: bool = false;

    fn to_sock_addr(addr: &Self::Addr) -> io::Result<Addr>;

    fn socket_params(addr: &Self::Addr) -> (i32, i32, i32);

    fn submit_quickack(_driver: &mut DriverContext<'_, '_>, _fd: &Fd<'_>) -> bool {
        false
    }

    fn validate_stream_config(_config: Self::StreamConfig) -> io::Result<()> {
        Ok(())
    }

    /// Queues best-effort tuning without waiting for kernel completion.
    fn submit_stream_tuning(
        _driver: &mut DriverContext<'_, '_>,
        _config: Self::StreamConfig,
        _fd: &Fd<'_>,
    ) -> bool {
        true
    }

    fn apply_profile_defaults(_config: &mut Self::StreamConfig, _user_timeout: Option<Duration>) {}
}

pub trait ListenerTransport: Transport {
    type ListenerConfig: Default + Clone + 'static;

    fn bind_listener_slot<'d>(
        driver: &mut DriverContext<'_, 'd>,
        addr: &Self::Addr,
        backlog: i32,
        config: &Self::ListenerConfig,
    ) -> io::Result<(Fd<'d>, SocketAddr)>;

    fn per_ip_limit(_config: &Self::ListenerConfig) -> Option<u32> {
        None
    }
}
