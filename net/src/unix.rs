use std::io;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::Transport;
use crate::option::StreamOption;
use dope_core::driver::DriverContext;
use dope_core::io::fd::Fd;
use dope_core::io::socket::addr::Addr;
use libc::AF_UNIX;
use libc::SOCK_STREAM;

pub mod listener {
    #[derive(Clone, Copy, Default)]
    pub struct Config;
}

pub mod stream {
    #[derive(Clone, Copy, Default)]
    pub struct Config {
        pub recv_buffer_size: Option<usize>,
        pub send_buffer_size: Option<usize>,
    }
}

use listener::Config;

pub struct Unix;

impl Unix {
    fn stream_options(config: stream::Config) -> [Option<StreamOption>; 2] {
        [
            config.recv_buffer_size.map(StreamOption::RecvBuffer),
            config.send_buffer_size.map(StreamOption::SendBuffer),
        ]
    }
}

impl Transport for Unix {
    type Addr = PathBuf;
    type StreamConfig = stream::Config;
    type ListenerConfig = Config;

    fn to_sock_addr(addr: &PathBuf) -> io::Result<Addr> {
        Addr::from_unix_path(addr)
    }

    fn socket_params(_addr: &PathBuf) -> (i32, i32, i32) {
        (AF_UNIX, SOCK_STREAM, 0)
    }

    fn bind_listener_slot<'d>(
        _driver: &mut DriverContext<'_, 'd>,
        _addr: &PathBuf,
        backlog: i32,
        _config: &Config,
    ) -> io::Result<(Fd<'d>, SocketAddr)> {
        if backlog <= 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "backlog must be > 0"));
        }
        Err(Error::new(
            ErrorKind::Unsupported,
            "dope: unix listener fixed-slot bootstrap not yet wired",
        ))
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
            config.recv_buffer_size.map(StreamOption::RecvBuffer),
            driver,
            fd,
        ) && StreamOption::submit(
            config.send_buffer_size.map(StreamOption::SendBuffer),
            driver,
            fd,
        )
    }
}
