use std::{io, net, os::fd::AsFd as _, time};

use dope_core::dope_sys::io::poll;

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
        let socket =
            net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).map_err(peer::Spawn::Io)?;
        let addr = socket.local_addr().map_err(peer::Spawn::Io)?;
        socket.set_nonblocking(true).map_err(peer::Spawn::Io)?;
        scripts::Script::with(
            addr,
            move |control| {
                let socket = Socket { socket, control };
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
    socket: net::UdpSocket,
    control: &'a scripts::Control,
}

impl Socket<'_> {
    pub fn recv_from(&self, output: &mut [u8]) -> io::Result<(usize, net::SocketAddr)> {
        self.io(poll::Entry::readable(self.socket.as_fd()), None, |socket| {
            socket.recv_from(output)
        })
    }

    pub fn send_to(&self, input: &[u8], target: net::SocketAddr) -> io::Result<usize> {
        self.io(
            poll::Entry::writable(self.socket.as_fd()),
            Some(GUARD),
            |socket| socket.send_to(input, target),
        )
    }

    fn io<T>(
        &self,
        entry: poll::Entry<'_>,
        timeout: Option<time::Duration>,
        mut run: impl FnMut(&net::UdpSocket) -> io::Result<T>,
    ) -> io::Result<T> {
        let start = time::Instant::now();
        loop {
            let left = timeout.map(|limit| limit.saturating_sub(start.elapsed()));
            if !self.control.ready(entry, left)? {
                return Err(io::ErrorKind::TimedOut.into());
            }
            match run(&self.socket) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) =>
                {
                    if timeout.is_some_and(|limit| start.elapsed() >= limit) {
                        return Err(io::ErrorKind::TimedOut.into());
                    }
                }
                result => return result,
            }
        }
    }
}
