use std::{cell::Cell, net, os::fd, time};

use dope_core::{
    Backend, Errno,
    dope_sys::{
        Nonblock,
        io::{
            fd::Fd as _,
            poll,
            socket::{self, Socket as _, addr},
        },
    },
};
use fd::AsFd as _;
use o3::base::marker::ThreadBound;

use crate::harness::peer::{Error, IO_TIMEOUT, scripts, scripts::Control};

mod raw;

/// An owned socket whose blocking operations also observe its worker's cancellation.
#[derive(Debug)]
pub struct Stream<'a> {
    socket: fd::OwnedFd,
    control: Option<&'a Control>,
    read: Cell<Option<time::Duration>>,
    write: Cell<Option<time::Duration>>,
    nonblocking: Cell<bool>,
    _thread: ThreadBound,
}

impl<'a> Stream<'a> {
    pub(super) fn new(socket: fd::OwnedFd, control: Option<&'a Control>) -> Result<Self, Error> {
        Backend::update::<Nonblock>(socket.as_fd())
            .map_err(socket::Setup::nonblock)
            .map_err(Error::Setup)?;
        if control.is_some() {
            Backend::set_option::<Errno>(socket.as_fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY, &1)?;
        }
        Ok(Self {
            socket,
            control,
            read: Cell::new(control.map(|_| IO_TIMEOUT)),
            write: Cell::new(control.map(|_| IO_TIMEOUT)),
            nonblocking: Cell::new(false),
            _thread: ThreadBound::NEW,
        })
    }

    pub fn shutdown(&self, how: net::Shutdown) -> Result<(), Error> {
        let how = match how {
            net::Shutdown::Read => socket::How::READ,
            net::Shutdown::Write => socket::How::WRITE,
            net::Shutdown::Both => socket::How::BOTH,
        };
        Backend::shutdown(self.socket.as_fd(), how)
    }

    pub fn peer_addr(&self) -> Result<net::SocketAddr, Error> {
        let mut address = addr::Storage::empty();
        Backend::peer_addr::<Errno>(self.socket.as_fd(), &mut address)?;
        address.to_std().map_err(Error::Address)
    }

    pub fn set_nodelay(&self, value: bool) -> Result<(), Error> {
        Backend::set_option(
            self.socket.as_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &i32::from(value),
        )
    }

    pub fn nonblock(&self, value: bool) {
        self.nonblocking.set(value);
    }

    pub fn read_wait(&self, timeout: Option<time::Duration>) -> Result<(), Error> {
        scripts::timeout(&self.read, timeout)
    }

    pub fn write_wait(&self, timeout: Option<time::Duration>) -> Result<(), Error> {
        scripts::timeout(&self.write, timeout)
    }

    fn io<T>(
        &self,
        entry: poll::Entry<'_>,
        timeout: Option<time::Duration>,
        run: impl FnMut() -> Result<T, Errno>,
    ) -> Result<T, Error> {
        scripts::io(self.control, entry, timeout, self.nonblocking.get(), run)
    }

    pub fn read(&self, output: &mut [u8]) -> Result<usize, Error> {
        self.receive::<0>(output)
    }

    pub fn peek(&self, output: &mut [u8]) -> Result<usize, Error> {
        self.receive::<{ libc::MSG_PEEK }>(output)
    }

    fn receive<const FLAGS: libc::c_int>(&self, output: &mut [u8]) -> Result<usize, Error> {
        if output.is_empty() {
            return Ok(0);
        }
        self.io(
            poll::Entry::readable(self.socket.as_fd()),
            self.read.get(),
            || Backend::recv::<FLAGS, Errno>(self.socket.as_fd(), output).map(|bytes| bytes.len()),
        )
    }

    pub fn write(&self, input: &[u8]) -> Result<usize, Error> {
        if input.is_empty() {
            return Ok(0);
        }
        self.io(
            poll::Entry::writable(self.socket.as_fd()),
            self.write.get(),
            || Backend::send_quiet::<Errno>(self.socket.as_fd(), input).map(|bytes| bytes.len()),
        )
    }

    pub fn read_exact(&self, mut output: &mut [u8]) -> Result<(), Error> {
        while !output.is_empty() {
            let count = self.read(output)?;
            if count == 0 {
                return Err(Error::UnexpectedEof);
            }
            output = &mut output[count..];
        }
        Ok(())
    }

    pub fn write_all(&self, mut input: &[u8]) -> Result<(), Error> {
        while !input.is_empty() {
            let count = self.write(input)?;
            if count == 0 {
                return Err(Error::WriteZero);
            }
            input = &input[count..];
        }
        Ok(())
    }

    pub fn read_to_end(&self, output: &mut Vec<u8>) -> Result<usize, Error> {
        let start = output.len();
        let mut bytes = [0; 4096];
        loop {
            let count = self.read(&mut bytes)?;
            if count == 0 {
                return Ok(output.len() - start);
            }
            output.extend_from_slice(&bytes[..count]);
        }
    }
}

impl fd::AsFd for Stream<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.socket.as_fd()
    }
}
