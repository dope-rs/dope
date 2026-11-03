use std::{cell::Cell, net, os::fd, time};

use dope_core::{
    Backend, Errno,
    dope_sys::io::{
        poll,
        socket::{self, Socket as _, addr},
    },
};
use fd::AsFd as _;
use o3::base::marker::ThreadBound;

use crate::harness::{GUARD, peer, peer::scripts};

/// A borrowing view of one scoped UDP peer.
#[must_use = "call finish or stop to observe the script outcome"]
pub struct Script<'scope, T> {
    inner: scripts::Script<'scope, T>,
}

impl<T: Send> Script<'_, T> {
    pub fn with<R>(
        script: impl FnOnce(&Socket<'_>) -> T + Send,
        run: impl for<'scope> FnOnce(Script<'scope, T>) -> R,
    ) -> Result<R, peer::Spawn> {
        let socket = peer::bind((net::Ipv4Addr::LOCALHOST, 0).into(), socket::Kind::Dgram)
            .map_err(peer::Spawn::Io)?;
        let addr = peer::local(socket.as_fd()).map_err(peer::Spawn::Io)?;
        scripts::Script::with(
            addr,
            move |control| {
                let socket = Socket::new(socket, Some(control));
                Ok(script(&socket))
            },
            |inner| run(Script { inner }),
        )
    }
}

impl<T> Script<'_, T> {
    pub const fn addr(&self) -> net::SocketAddr {
        self.inner.addr
    }

    pub fn finish(self) -> Result<T, peer::Finish> {
        self.inner.finish()
    }

    pub fn stop(self) -> Result<T, peer::Finish> {
        self.inner.stop()
    }
}

/// Worker-owned UDP operations with a retained cancellation endpoint.
pub struct Socket<'a> {
    socket: fd::OwnedFd,
    control: Option<&'a scripts::Control>,
    read: Cell<Option<time::Duration>>,
    write: Cell<Option<time::Duration>>,
    nonblocking: Cell<bool>,
    _thread: ThreadBound,
}

impl Socket<'static> {
    pub fn bind(address: net::SocketAddr) -> Result<Self, peer::Error> {
        Ok(Self::new(peer::bind(address, socket::Kind::Dgram)?, None))
    }

    /// Consumes one loopback datagram socket, closing it if admission fails.
    pub fn try_adopt(socket: fd::OwnedFd) -> Result<Self, peer::Error> {
        Ok(Self::new(peer::adopt(socket, socket::Kind::Dgram)?, None))
    }
}

impl<'a> Socket<'a> {
    fn new(socket: fd::OwnedFd, control: Option<&'a scripts::Control>) -> Self {
        Self {
            socket,
            control,
            read: Cell::new(None),
            write: Cell::new(control.map(|_| GUARD)),
            nonblocking: Cell::new(false),
            _thread: ThreadBound::NEW,
        }
    }

    pub fn local_addr(&self) -> Result<net::SocketAddr, peer::Error> {
        peer::local(self.socket.as_fd())
    }

    pub fn read_wait(&self, timeout: Option<time::Duration>) -> Result<(), peer::Error> {
        scripts::timeout(&self.read, timeout)
    }

    pub fn write_wait(&self, timeout: Option<time::Duration>) -> Result<(), peer::Error> {
        scripts::timeout(&self.write, timeout)
    }

    pub fn nonblock(&self, value: bool) {
        self.nonblocking.set(value);
    }

    pub fn recv(&self, output: &mut [u8]) -> Result<usize, peer::Error> {
        scripts::io(
            self.control,
            poll::Entry::readable(self.socket.as_fd()),
            self.read.get(),
            self.nonblocking.get(),
            || Backend::recv::<0, Errno>(self.socket.as_fd(), output).map(|bytes| bytes.len()),
        )
    }

    pub fn recv_from(&self, output: &mut [u8]) -> Result<(usize, net::SocketAddr), peer::Error> {
        let mut address = addr::Storage::empty();
        let count = scripts::io(
            self.control,
            poll::Entry::readable(self.socket.as_fd()),
            self.read.get(),
            self.nonblocking.get(),
            || {
                Backend::recv_from::<Errno>(self.socket.as_fd(), output, &mut address)
                    .map(|bytes| bytes.len())
            },
        )?;
        Ok((count, address.to_std().map_err(peer::Error::Address)?))
    }

    pub fn send_to(&self, input: &[u8], target: net::SocketAddr) -> Result<usize, peer::Error> {
        let address = addr::Internet::from_std::<Backend>(target);
        scripts::io(
            self.control,
            poll::Entry::writable(self.socket.as_fd()),
            self.write.get(),
            self.nonblocking.get(),
            || {
                Backend::send_to::<Errno>(self.socket.as_fd(), input, address.as_addr())
                    .map(|bytes| bytes.len())
            },
        )
    }
}

impl fd::AsFd for Socket<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.socket.as_fd()
    }
}
