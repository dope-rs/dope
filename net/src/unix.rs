use std::{io, path};

use dope_core::io::socket::{self, option};

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Addr(socket::Addr);

impl Addr {
    pub fn from_path(path: &path::Path) -> io::Result<Self> {
        Ok(Self(socket::Addr::from_unix_path(path)?))
    }
}

#[derive(Clone, Copy, Default)]
pub struct StreamConfig {
    pub recv_buffer_size: Option<usize>,
    pub send_buffer_size: Option<usize>,
}

pub struct Unix;

impl crate::Transport for Unix {
    type Addr = Addr;
    type StreamConfig = StreamConfig;

    fn to_sock_addr(addr: &Addr) -> io::Result<socket::Addr> {
        Ok(addr.0)
    }

    fn stream_options(config: StreamConfig) -> io::Result<option::StreamOptions> {
        [
            config.recv_buffer_size.map(option::Stream::Buffer),
            config.send_buffer_size.map(option::Stream::SendBuffer),
        ]
        .try_into()
    }
}
