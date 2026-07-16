use std::io;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::time::Duration;

use crate::Transport;
use crate::option::SocketOption;
use dope_core::backend::Sqe;
use dope_core::driver::DriverContext;
use dope_core::driver::bootstrap::Bootstrap;
use dope_core::driver::submission::Submission;
use dope_core::io::fd::Fd;
use dope_core::io::socket::addr::Addr;
use dope_core::io::socket::{Domain, Kind, ListenerConfig};

pub mod listener {
    use std::time::Duration;

    #[derive(Clone, Copy, Default)]
    pub struct Config {
        pub reuse_port: bool,
        pub fast_open_backlog: Option<u32>,
        pub defer_accept: Option<Duration>,
        pub per_ip_limit: Option<u32>,
    }
}

pub mod stream {
    use std::time::Duration;

    #[derive(Clone, Copy, Default)]
    pub struct Config {
        pub recv_buffer_size: Option<usize>,
        pub send_buffer_size: Option<usize>,
        pub quick_ack: Option<bool>,
        pub no_delay: Option<bool>,
        pub keep_alive: Option<bool>,
        pub keep_alive_idle: Option<Duration>,
        pub keep_alive_interval: Option<Duration>,
        pub keep_alive_retries: Option<u32>,
        pub user_timeout: Option<Duration>,
    }

    impl Config {
        pub(super) const fn keep_alive_tuned(self) -> bool {
            self.keep_alive_idle.is_some()
                || self.keep_alive_interval.is_some()
                || self.keep_alive_retries.is_some()
        }
    }
}

pub struct Tcp;

impl Tcp {
    fn listener_config(config: &listener::Config) -> io::Result<ListenerConfig> {
        let defer_accept_secs = config
            .defer_accept
            .map(|duration| u32::try_from(duration.as_secs()))
            .transpose()
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "defer_accept exceeds u32 seconds"))?;
        Ok(ListenerConfig {
            reuse_addr: true,
            reuse_port: config.reuse_port,
            fast_open_backlog: config.fast_open_backlog,
            defer_accept_secs,
        })
    }
}

impl Transport for Tcp {
    type Addr = SocketAddr;
    type StreamConfig = stream::Config;
    type ListenerConfig = listener::Config;

    const KERNEL_DISCARD: bool = true;

    fn to_sock_addr(addr: &SocketAddr) -> io::Result<Addr> {
        Ok(Addr::from_std(*addr))
    }

    fn socket_params(addr: &SocketAddr) -> (i32, i32, i32) {
        (Domain::for_addr(addr).raw(), Kind::Stream.raw(), 0)
    }

    fn bind_listener_slot<'d>(
        driver: &mut DriverContext<'_, 'd>,
        addr: &SocketAddr,
        backlog: i32,
        config: &listener::Config,
    ) -> io::Result<(Fd<'d>, SocketAddr)> {
        if backlog <= 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "backlog must be > 0"));
        }
        let config = Self::listener_config(config)?;
        driver.bind_listener_slot(*addr, backlog, &config)
    }

    fn submit_stream_config(
        driver: &mut DriverContext<'_, '_>,
        config: stream::Config,
        fd: &Fd<'_>,
    ) {
        let tuned = config.keep_alive_tuned();
        SocketOption::submit_all(
            [
                config.quick_ack.map(SocketOption::QuickAck),
                config.no_delay.map(SocketOption::NoDelay),
                config.keep_alive.map(SocketOption::KeepAlive),
                config.recv_buffer_size.map(SocketOption::RecvBuffer),
                config.send_buffer_size.map(SocketOption::SendBuffer),
                config
                    .keep_alive_idle
                    .filter(|_| tuned)
                    .map(SocketOption::KeepAliveIdle),
                config
                    .keep_alive_interval
                    .filter(|_| tuned)
                    .map(SocketOption::KeepAliveInterval),
                config
                    .keep_alive_retries
                    .filter(|_| tuned)
                    .map(SocketOption::KeepAliveRetries),
                config.user_timeout.map(SocketOption::UserTimeout),
            ],
            driver,
            fd,
        );
    }

    fn per_ip_limit(config: &listener::Config) -> Option<u32> {
        config.per_ip_limit
    }

    fn submit_quickack(driver: &mut DriverContext<'_, '_>, fd: &Fd<'_>) -> bool {
        driver.push(Sqe::quickack(fd)).is_ok()
    }

    fn apply_profile_defaults(config: &mut stream::Config, user_timeout: Option<Duration>) {
        config.user_timeout = config.user_timeout.or(user_timeout);
    }
}
