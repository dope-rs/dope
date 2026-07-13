use std::io;
use std::net::SocketAddr;

use crate::transport::Transport;
use crate::transport::config::tcp;
use crate::{Bootstrap, Drive, Driver, backend};

pub struct Tcp;

impl Tcp {
    fn map_listener_opts(cfg: &tcp::ListenerOpts) -> backend::ListenerOpts {
        backend::ListenerOpts {
            reuse_addr: true,
            reuse_port: cfg.reuseport.flag().unwrap_or(false),
            fastopen_backlog: cfg.fastopen_backlog,
            defer_accept_secs: cfg.defer_accept_secs,
        }
    }
}

impl Transport for Tcp {
    type Addr = SocketAddr;
    type StreamOpts = tcp::StreamOpts;
    type ListenerOpts = tcp::ListenerOpts;

    const KERNEL_DISCARD: bool = true;

    fn to_sock_addr(addr: SocketAddr) -> io::Result<backend::socket::Addr> {
        Ok(backend::socket::Addr::from_std(addr))
    }

    fn socket_params(addr: &SocketAddr) -> (i32, i32, i32) {
        (
            backend::socket::Domain::for_addr(addr).raw(),
            backend::socket::Kind::Stream.raw(),
            0,
        )
    }

    fn bind_listener_slot<'d>(
        driver: &'d Driver,
        addr: &SocketAddr,
        backlog: i32,
        cfg: &tcp::ListenerOpts,
    ) -> io::Result<(backend::socket::Fd<'d>, SocketAddr)> {
        if backlog <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backlog must be > 0",
            ));
        }
        let opts = Self::map_listener_opts(cfg);
        driver.bind_listener_slot(*addr, backlog, &opts)
    }

    fn per_ip_cap(cfg: &tcp::ListenerOpts) -> Option<u32> {
        cfg.per_ip_cap
    }

    fn submit_quickack<'d>(fd: &backend::socket::Fd<'d>, driver: &'d Driver) -> bool {
        driver.push(backend::sqe::Sqe::quickack(fd)).is_ok()
    }

    fn apply_profile_defaults(
        opts: &mut tcp::StreamOpts,
        user_timeout: Option<std::time::Duration>,
    ) {
        opts.user_timeout = opts.user_timeout.or(user_timeout);
    }
}
