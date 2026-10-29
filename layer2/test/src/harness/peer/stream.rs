use std::{cell::Cell, io, net, os::fd::AsFd as _, time};

use dope_core::dope_sys::io::poll;

use crate::harness::peer::{IO_TIMEOUT, scripts::Control};

/// A worker-owned socket whose blocking operations also observe cancellation.
pub struct Stream<'a> {
    socket: net::TcpStream,
    control: &'a Control,
    read: Cell<Option<time::Duration>>,
    write: Cell<Option<time::Duration>>,
    nonblocking: Cell<bool>,
    stopped: Cell<bool>,
}

impl<'a> Stream<'a> {
    pub(super) fn new(socket: net::TcpStream, control: &'a Control) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            control,
            read: Cell::new(Some(IO_TIMEOUT)),
            write: Cell::new(Some(IO_TIMEOUT)),
            nonblocking: Cell::new(false),
            stopped: Cell::new(false),
        })
    }

    pub fn shutdown(&self, how: net::Shutdown) -> io::Result<()> {
        self.socket.shutdown(how)
    }

    pub fn peer_addr(&self) -> io::Result<net::SocketAddr> {
        self.socket.peer_addr()
    }

    pub fn set_nodelay(&self, value: bool) -> io::Result<()> {
        self.socket.set_nodelay(value)
    }

    pub fn reset_on_close(&self) -> io::Result<()> {
        super::raw::reset(self.socket.as_fd())
    }

    /// Whether this stream has observed its owner's cancellation during I/O.
    /// Native socket failures never establish this witness.
    pub fn is_stopped(&self) -> bool {
        self.stopped.get()
    }

    pub fn nonblock(&self, value: bool) {
        self.nonblocking.set(value);
    }

    pub fn read_wait(&self, timeout: Option<time::Duration>) -> io::Result<()> {
        Self::set_timeout(&self.read, timeout)
    }

    pub fn write_wait(&self, timeout: Option<time::Duration>) -> io::Result<()> {
        Self::set_timeout(&self.write, timeout)
    }

    fn set_timeout(
        slot: &Cell<Option<time::Duration>>,
        timeout: Option<time::Duration>,
    ) -> io::Result<()> {
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        slot.set(timeout);
        Ok(())
    }

    fn io<T>(
        &self,
        entry: poll::Entry<'_>,
        timeout: Option<time::Duration>,
        mut run: impl FnMut(&net::TcpStream) -> io::Result<T>,
    ) -> io::Result<T> {
        let start = time::Instant::now();
        let nowait = self.nonblocking.get();
        let mut waiting = false;
        loop {
            let ready = if waiting {
                let left = timeout.map(|limit| limit.saturating_sub(start.elapsed()));
                self.control.ready(entry, left)
            } else {
                self.control.check().map(|()| true)
            }
            .map_err(|error| {
                if error.kind() == io::ErrorKind::Interrupted {
                    self.stopped.set(true);
                    io::ErrorKind::ConnectionAborted.into()
                } else {
                    error
                }
            })?;
            if !ready && !nowait {
                return Err(io::ErrorKind::TimedOut.into());
            }
            match run(&self.socket) {
                Err(error)
                    if error.kind() == io::ErrorKind::Interrupted
                        || (error.kind() == io::ErrorKind::WouldBlock && !nowait) =>
                {
                    if !nowait && timeout.is_some_and(|limit| start.elapsed() >= limit) {
                        return Err(io::ErrorKind::TimedOut.into());
                    }
                    waiting = error.kind() == io::ErrorKind::WouldBlock;
                }
                result => return result,
            }
        }
    }
}

impl io::Read for &Stream<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        self.io(
            poll::Entry::readable(self.socket.as_fd()),
            self.read.get(),
            |mut socket| socket.read(output),
        )
    }

    fn read_vectored(&mut self, output: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        if output.iter().all(|slice| slice.is_empty()) {
            return Ok(0);
        }
        self.io(
            poll::Entry::readable(self.socket.as_fd()),
            self.read.get(),
            |mut socket| socket.read_vectored(output),
        )
    }
}

impl io::Write for &Stream<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }
        self.io(
            poll::Entry::writable(self.socket.as_fd()),
            self.write.get(),
            |mut socket| socket.write(input),
        )
    }

    fn write_vectored(&mut self, input: &[io::IoSlice<'_>]) -> io::Result<usize> {
        if input.iter().all(|slice| slice.is_empty()) {
            return Ok(0);
        }
        self.io(
            poll::Entry::writable(self.socket.as_fd()),
            self.write.get(),
            |mut socket| socket.write_vectored(input),
        )
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Read for Stream<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut &*self, output)
    }

    fn read_vectored(&mut self, output: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        io::Read::read_vectored(&mut &*self, output)
    }
}

impl io::Write for Stream<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        io::Write::write(&mut &*self, input)
    }

    fn write_vectored(&mut self, input: &[io::IoSlice<'_>]) -> io::Result<usize> {
        io::Write::write_vectored(&mut &*self, input)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
