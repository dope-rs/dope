pub mod config;
pub mod link;
pub mod multishot;
mod tcp;
mod unix;
pub mod wire;

use std::io;
use std::net::SocketAddr;

use config::Submittable;
pub use tcp::Tcp;
pub use unix::Unix;

use crate::{Drive, Driver, backend};

pub trait Transport: 'static + Sized {
    type Addr;
    type StreamOpts: Default + Copy + 'static + Submittable;
    type ListenerOpts: Default + Clone + 'static;

    /// `recv(MSG_TRUNC)` frees bytes in-kernel without writing the buffer — true
    /// for TCP, false for AF_UNIX (which always copies).
    const KERNEL_DISCARD: bool = false;

    fn to_sock_addr(addr: Self::Addr) -> io::Result<backend::socket::Addr>;

    fn socket_params(addr: &Self::Addr) -> (i32, i32, i32);

    fn bind_listener_slot(
        driver: &mut Driver,
        addr: &Self::Addr,
        backlog: i32,
        opts: &Self::ListenerOpts,
    ) -> io::Result<(backend::socket::Fd, SocketAddr)>;

    fn submit_shutdown(fd: &backend::socket::Fd, how: i32, driver: &mut Driver) -> bool {
        driver.push(backend::sqe::Sqe::shutdown(fd, how)).is_ok()
    }

    fn submit_quickack(fd: &backend::socket::Fd, driver: &mut Driver) -> bool {
        let _ = (fd, driver);
        false
    }

    fn per_ip_cap(_opts: &Self::ListenerOpts) -> Option<u32> {
        None
    }

    fn apply_profile_defaults(
        _opts: &mut Self::StreamOpts,
        _user_timeout: Option<std::time::Duration>,
    ) {
    }
}
