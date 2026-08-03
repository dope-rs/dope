use std::io;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::time::Duration;

use dope_core::backend::Sqe;
use dope_core::driver::DriverContext;
use dope_core::driver::bootstrap::Bootstrap;
use dope_core::driver::submission::Submission;
use dope_core::io::fd::Fd;
use dope_core::io::socket::addr::Addr;
use dope_core::io::socket::{Domain, Kind, ListenerConfig};

use crate::option::StreamOption;
use crate::{ListenerTransport, Transport};

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
}

use listener::Config;

trait ListenerPlatform {
    fn listener_options(&self) -> io::Result<(Option<i32>, Option<i32>)>;
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{Config, Error, ErrorKind, ListenerPlatform, StreamOption, io};

    impl ListenerPlatform for Config {
        fn listener_options(&self) -> io::Result<(Option<i32>, Option<i32>)> {
            let fast_open_backlog = self
                .fast_open_backlog
                .map(i32::try_from)
                .transpose()
                .map_err(|_| {
                    Error::new(ErrorKind::InvalidInput, "fast-open backlog exceeds c_int")
                })?;
            let defer_accept_secs = self
                .defer_accept
                .map(|duration| {
                    StreamOption::seconds_raw(duration).ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidInput,
                            "deferred accept must fit positive c_int seconds",
                        )
                    })
                })
                .transpose()?;
            Ok((fast_open_backlog, defer_accept_secs))
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{Config, Error, ErrorKind, ListenerPlatform, io};

    impl ListenerPlatform for Config {
        fn listener_options(&self) -> io::Result<(Option<i32>, Option<i32>)> {
            if self.fast_open_backlog.is_some() || self.defer_accept.is_some() {
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "TCP fast open and deferred accept require Linux",
                ))
            } else {
                Ok((None, None))
            }
        }
    }
}

pub struct Tcp;

impl Tcp {
    fn listener_config(config: &Config) -> io::Result<ListenerConfig> {
        let (fast_open_backlog, defer_accept_secs) = config.listener_options()?;
        Ok(ListenerConfig {
            reuse_addr: true,
            reuse_port: config.reuse_port,
            fast_open_backlog,
            defer_accept_secs,
        })
    }

    fn stream_options(config: stream::Config) -> [Option<StreamOption>; 9] {
        [
            config.quick_ack.map(StreamOption::QuickAck),
            config.no_delay.map(StreamOption::NoDelay),
            config.keep_alive.map(StreamOption::KeepAlive),
            config.recv_buffer_size.map(StreamOption::RecvBuffer),
            config.send_buffer_size.map(StreamOption::SendBuffer),
            config.keep_alive_idle.map(StreamOption::KeepAliveIdle),
            config
                .keep_alive_interval
                .map(StreamOption::KeepAliveInterval),
            config
                .keep_alive_retries
                .map(StreamOption::KeepAliveRetries),
            config.user_timeout.map(StreamOption::UserTimeout),
        ]
    }
}

impl Transport for Tcp {
    type Addr = SocketAddr;
    type StreamConfig = stream::Config;

    const KERNEL_DISCARD: bool = true;

    fn to_sock_addr(addr: &SocketAddr) -> io::Result<Addr> {
        Ok(Addr::from_std(*addr))
    }

    fn socket_params(addr: &SocketAddr) -> (i32, i32, i32) {
        (Domain::for_addr(addr).raw(), Kind::Stream.raw(), 0)
    }

    fn submit_quickack(driver: &mut DriverContext<'_, '_>, fd: &Fd<'_>) -> bool {
        driver.push(Sqe::quickack(fd)).is_ok()
    }

    fn validate_stream_config(config: stream::Config) -> io::Result<()> {
        StreamOption::validate_all(Self::stream_options(config))
    }

    fn submit_stream_tuning(
        driver: &mut DriverContext<'_, '_>,
        config: stream::Config,
        fd: &Fd<'_>,
    ) -> bool {
        StreamOption::submit(
            config.user_timeout.map(StreamOption::UserTimeout),
            driver,
            fd,
        ) && StreamOption::submit(config.quick_ack.map(StreamOption::QuickAck), driver, fd)
            && StreamOption::submit(config.no_delay.map(StreamOption::NoDelay), driver, fd)
            && StreamOption::submit(config.keep_alive.map(StreamOption::KeepAlive), driver, fd)
            && StreamOption::submit(
                config.recv_buffer_size.map(StreamOption::RecvBuffer),
                driver,
                fd,
            )
            && StreamOption::submit(
                config.send_buffer_size.map(StreamOption::SendBuffer),
                driver,
                fd,
            )
            && StreamOption::submit(
                config.keep_alive_idle.map(StreamOption::KeepAliveIdle),
                driver,
                fd,
            )
            && StreamOption::submit(
                config
                    .keep_alive_interval
                    .map(StreamOption::KeepAliveInterval),
                driver,
                fd,
            )
            && StreamOption::submit(
                config
                    .keep_alive_retries
                    .map(StreamOption::KeepAliveRetries),
                driver,
                fd,
            )
    }

    fn apply_profile_defaults(config: &mut stream::Config, user_timeout: Option<Duration>) {
        if StreamOption::supports_user_timeout() {
            config.user_timeout = config.user_timeout.or(user_timeout);
        }
    }
}

impl ListenerTransport for Tcp {
    type ListenerConfig = Config;

    fn bind_listener_slot<'d>(
        driver: &mut DriverContext<'_, 'd>,
        addr: &SocketAddr,
        backlog: i32,
        config: &Config,
    ) -> io::Result<(Fd<'d>, SocketAddr)> {
        if backlog <= 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "backlog must be > 0"));
        }
        let config = Self::listener_config(config)?;
        driver.bind_listener_slot(*addr, backlog, &config)
    }

    fn per_ip_limit(config: &Config) -> Option<u32> {
        config.per_ip_limit
    }
}
