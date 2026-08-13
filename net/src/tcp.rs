use std::{io, net, time};

use dope_core::{
    driver,
    io::{
        fd::handles,
        socket::{self, option},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FastOpenBacklog(i32);

impl FastOpenBacklog {
    pub const fn new(backlog: i32) -> Option<Self> {
        if backlog > 0 {
            Some(Self(backlog))
        } else {
            None
        }
    }

    pub const fn get(self) -> i32 {
        self.0
    }

    fn into_core(self) -> Option<socket::FastOpenBacklog> {
        socket::FastOpenBacklog::new(self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FastOpen {
    #[default]
    Disabled,
    Required {
        backlog: FastOpenBacklog,
    },
}

impl FastOpen {
    fn into_core(self) -> Option<socket::FastOpen> {
        match self {
            Self::Disabled => Some(socket::FastOpen::Disabled),
            Self::Required { backlog } => Some(socket::FastOpen::Required {
                backlog: backlog.into_core()?,
            }),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct ListenerConfig {
    pub reuse_port: bool,
    pub fast_open: FastOpen,
    pub per_ip_limit: Option<u32>,
}

#[derive(Clone, Copy, Default)]
pub struct StreamConfig {
    pub recv_buffer_size: Option<usize>,
    pub send_buffer_size: Option<usize>,
    pub no_delay: Option<bool>,
    pub keep_alive: Option<bool>,
    pub keep_alive_idle: Option<time::Duration>,
    pub keep_alive_interval: Option<time::Duration>,
    pub keep_alive_retries: Option<u32>,
}

pub struct Tcp;

impl crate::Transport for Tcp {
    type Addr = net::SocketAddr;
    type StreamConfig = StreamConfig;

    fn to_sock_addr(addr: &net::SocketAddr) -> io::Result<socket::Addr> {
        Ok(socket::Addr::from_std(*addr))
    }

    fn stream_options(config: StreamConfig) -> io::Result<option::StreamOptions> {
        [
            config.no_delay.map(option::Stream::NoDelay),
            config.keep_alive.map(option::Stream::KeepAlive),
            config.recv_buffer_size.map(option::Stream::Buffer),
            config.send_buffer_size.map(option::Stream::SendBuffer),
            config.keep_alive_idle.map(option::Stream::KeepAliveIdle),
            config
                .keep_alive_interval
                .map(option::Stream::KeepAliveInterval),
            config
                .keep_alive_retries
                .map(option::Stream::KeepAliveRetries),
        ]
        .try_into()
    }
}

impl crate::ListenerTransport for Tcp {
    type ListenerConfig = ListenerConfig;

    fn bind_listener_slot<'d>(
        driver: &mut driver::Context<'_, 'd>,
        addr: &net::SocketAddr,
        backlog: i32,
        config: &ListenerConfig,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)> {
        use dope_core::driver::ops;

        if backlog <= 0 {
            use std::io::{Error, ErrorKind};
            return Err(Error::new(ErrorKind::InvalidInput, "backlog must be > 0"));
        }
        let fast_open = config.fast_open.into_core().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid Fast Open backlog")
        })?;
        let config = socket::ListenerConfig::for_tcp(config.reuse_port, fast_open);
        <driver::Context<'_, 'd> as ops::Bootstrap<'d>>::bind_listener_slot(
            driver, *addr, backlog, &config,
        )
    }

    fn per_ip_limit(config: &ListenerConfig) -> Option<u32> {
        config.per_ip_limit
    }
}
