use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::transport::Transport;
use crate::transport::config::unix;
use crate::{Driver, backend};

pub struct Unix;

impl Transport for Unix {
    type Addr = PathBuf;
    type StreamOpts = unix::StreamOpts;
    type ListenerOpts = unix::ListenerOpts;

    fn to_sock_addr(addr: PathBuf) -> io::Result<backend::socket::Addr> {
        backend::socket::Addr::from_unix_path(&addr)
    }

    fn socket_params(_addr: &PathBuf) -> (i32, i32, i32) {
        (libc::AF_UNIX, libc::SOCK_STREAM, 0)
    }

    fn bind_listener_slot(
        _driver: &mut Driver,
        _addr: &PathBuf,
        backlog: i32,
        _opts: &unix::ListenerOpts,
    ) -> io::Result<(backend::socket::Fd, SocketAddr)> {
        if backlog <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backlog must be > 0",
            ));
        }
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "dope: unix listener fixed-slot bootstrap not yet wired",
        ))
    }
}
