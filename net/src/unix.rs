use std::io;
use std::path::Path;

use dope_core::driver::DriverContext;
use dope_core::io::fd::Fd;
use dope_core::io::socket::addr;
use libc::{AF_UNIX, SOCK_STREAM};

use crate::Transport;
use crate::option::StreamOption;

pub mod stream {
    #[derive(Clone, Copy, Default)]
    pub struct Config {
        pub recv_buffer_size: Option<usize>,
        pub send_buffer_size: Option<usize>,
    }
}

pub struct Unix;

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Addr(addr::Addr);

impl Addr {
    pub fn from_path(path: &Path) -> io::Result<Self> {
        Ok(Self(addr::Addr::from_unix_path(path)?))
    }
}

impl Unix {
    fn stream_options(config: stream::Config) -> [Option<StreamOption>; 2] {
        [
            config.recv_buffer_size.map(StreamOption::RecvBuffer),
            config.send_buffer_size.map(StreamOption::SendBuffer),
        ]
    }
}

impl Transport for Unix {
    type Addr = Addr;
    type StreamConfig = stream::Config;

    fn to_sock_addr(addr: &Addr) -> io::Result<addr::Addr> {
        Ok(addr.0)
    }

    fn socket_params(_addr: &Addr) -> (i32, i32, i32) {
        (AF_UNIX, SOCK_STREAM, 0)
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
