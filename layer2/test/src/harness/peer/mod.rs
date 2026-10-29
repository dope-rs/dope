use std::{io, io::Write as _, mem, net, ops, os::fd::AsFd as _, time};

use dope_core::{dope_sys::io::poll, platform::wake};
use o3::depot::array::init;
use scripts::{Control, Script};

use crate::harness::scenario::run;

mod client;
mod connect;
mod finish;
mod half;
mod release;
mod scripts;
mod stream;
pub mod udp;

pub use client::Client;
pub use connect::Connect;
pub use finish::Finish;
pub use half::Half;
pub use release::Release;
pub use stream::Stream;

#[derive(Debug)]
pub enum Exchange {
    Write(io::Error),
    Read(io::Error),
}

#[derive(Debug)]
pub enum Spawn {
    Io(io::Error),
    Wake(wake::CreateError),
    Thread(run::Start),
}

const IO_TIMEOUT: time::Duration = time::Duration::from_secs(5);
const FINISH_TIMEOUT: time::Duration = time::Duration::from_secs(10);

impl<T: Send> TcpScript<'_, T> {
    /// Runs scripted peers after the case consumes its release permission.
    pub fn try_gated<const N: usize>(
        script: impl FnOnce(&mut [&Stream<'_>; N]) -> T + Send,
        run: impl FnOnce(net::SocketAddr, Release),
    ) -> Result<T, Finish> {
        let (wait, release) = wake::Ends::blocking()
            .map_err(Spawn::Wake)
            .map_err(Finish::Spawn)?
            .split();
        TcpScript::with_many::<N, _>(
            move |peers| {
                wait.wait_timeout(crate::harness::GUARD)?;
                Ok(script(peers))
            },
            |server| {
                run(server.addr(), Release(release));
                server.finish()?.map_err(Finish::Release)
            },
        )
        .map_err(Finish::Spawn)?
    }
}

#[derive(Clone, Copy)]
pub struct Peer {
    addr: net::SocketAddr,
}

impl Peer {
    pub const fn at(addr: net::SocketAddr) -> Self {
        Self { addr }
    }

    pub fn try_connect(self) -> io::Result<net::TcpStream> {
        net::TcpStream::connect_timeout(&self.addr, crate::harness::GUARD)
    }

    pub fn try_read(self, timeout: time::Duration) -> Result<net::TcpStream, Connect> {
        let stream = self.try_connect().map_err(Connect::Open)?;
        if let Err(error) = stream.set_read_timeout(Some(timeout)) {
            return Err(Connect::ReadTimeout { stream, error });
        }
        Ok(stream)
    }

    pub fn with<T: Send, R>(
        self,
        script: impl FnOnce(&mut &Stream<'_>) -> T + Send,
        run: impl for<'scope> FnOnce(Client<'scope, T>) -> R,
    ) -> Result<R, Spawn> {
        self.with_many::<1, T, R>(move |[stream]| script(stream), run)
    }

    pub fn with_many<const N: usize, T: Send, R>(
        self,
        script: impl FnOnce(&mut [&Stream<'_>; N]) -> T + Send,
        run: impl for<'scope> FnOnce(Client<'scope, T, N>) -> R,
    ) -> Result<R, Spawn> {
        nonempty::<N>();
        let addr = self.addr;
        Script::with(
            addr,
            move |control| {
                fixed(
                    || {
                        control.check()?;
                        let socket = net::TcpStream::connect_timeout(&addr, crate::harness::GUARD)?;
                        Stream::new(socket, control)
                    },
                    script,
                )
            },
            |inner| run(Client::new(inner)),
        )
    }

    pub fn exchange<R>(
        self,
        request: Vec<u8>,
        run: impl for<'scope> FnOnce(Client<'scope, Result<Vec<u8>, Exchange>>) -> R,
    ) -> Result<R, Spawn> {
        self.with(
            move |stream| {
                stream.write_all(&request).map_err(Exchange::Write)?;
                Self::try_read_all(stream).map_err(Exchange::Read)
            },
            run,
        )
    }

    pub fn try_read_all(mut stream: impl io::Read) -> io::Result<Vec<u8>> {
        let mut got = Vec::new();
        stream.read_to_end(&mut got)?;
        Ok(got)
    }

    pub fn try_reset(socket: net::TcpStream) -> io::Result<()> {
        raw::reset(socket.as_fd())
    }
}

mod raw {
    use std::{
        io, mem,
        os::fd::{AsRawFd as _, BorrowedFd},
    };

    /// Turns descriptor close into a reset.
    pub(super) fn reset(file: BorrowedFd<'_>) -> io::Result<()> {
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // SAFETY: the descriptor borrow and complete native value outlive the call.
        let status = unsafe {
            libc::setsockopt(
                file.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::from_ref(&linger).cast(),
                mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

/// A borrowing view of one scoped group of scripted TCP peers.
#[must_use = "call finish to observe the script outcome"]
pub struct TcpScript<'scope, T, const N: usize = 1> {
    inner: Script<'scope, T>,
}

impl<T: Send> TcpScript<'_, T> {
    pub fn with<R>(
        script: impl FnOnce(&mut &Stream<'_>) -> T + Send,
        run: impl for<'scope> FnOnce(TcpScript<'scope, T>) -> R,
    ) -> Result<R, Spawn> {
        Self::with_many::<1, R>(move |[stream]| script(stream), run)
    }

    pub fn with_many<const N: usize, R>(
        script: impl FnOnce(&mut [&Stream<'_>; N]) -> T + Send,
        run: impl for<'scope> FnOnce(TcpScript<'scope, T, N>) -> R,
    ) -> Result<R, Spawn> {
        nonempty::<N>();
        let (addr, listener) = bind_script().map_err(Spawn::Io)?;
        Script::with(
            addr,
            move |control| {
                let mut acceptor = TcpAcceptor { listener, control };
                fixed(|| acceptor.accept(), script)
            },
            |inner| run(TcpScript { inner }),
        )
    }
}

impl<T, const N: usize> TcpScript<'_, T, N> {
    pub const fn addr(&self) -> net::SocketAddr {
        self.inner.addr
    }

    pub fn finish(self) -> Result<T, Finish> {
        self.inner.finish()
    }
}

/// A borrowing view of one scoped listener and its accepted peers.
#[must_use = "call finish to observe the script outcome"]
pub struct AcceptScript<'scope, T> {
    inner: Script<'scope, T>,
}

impl<T: Send> AcceptScript<'_, T> {
    pub fn with<R>(
        script: impl FnOnce(&mut TcpAcceptor<'_>) -> T + Send,
        run: impl for<'scope> FnOnce(AcceptScript<'scope, T>) -> R,
    ) -> Result<R, Spawn> {
        let (addr, listener) = bind_script().map_err(Spawn::Io)?;
        Script::with(
            addr,
            move |control| {
                let mut acceptor = TcpAcceptor { listener, control };
                Ok(script(&mut acceptor))
            },
            |inner| run(AcceptScript { inner }),
        )
    }
}

impl<T> AcceptScript<'_, T> {
    pub const fn addr(&self) -> net::SocketAddr {
        self.inner.addr
    }

    pub fn finish(self) -> Result<T, Finish> {
        self.inner.finish()
    }

    pub fn stop(self) -> Result<T, Finish> {
        self.inner.stop()
    }
}

/// Sequential accepts on one worker-owned listener.
pub struct TcpAcceptor<'a> {
    listener: net::TcpListener,
    control: &'a Control,
}

impl<'a> TcpAcceptor<'a> {
    pub fn accept(&mut self) -> io::Result<Stream<'a>> {
        self.next(None)?.ok_or_else(|| io::ErrorKind::Other.into())
    }

    pub fn try_accept(&mut self) -> io::Result<Option<Stream<'a>>> {
        self.next(Some(time::Duration::ZERO))
    }

    pub fn accept_for(&mut self, timeout: time::Duration) -> io::Result<Option<Stream<'a>>> {
        self.next(Some(timeout))
    }

    fn next(&mut self, timeout: Option<time::Duration>) -> io::Result<Option<Stream<'a>>> {
        let start = time::Instant::now();
        loop {
            let left = timeout.map(|limit| limit.saturating_sub(start.elapsed()));
            if !self
                .control
                .ready(poll::Entry::readable(self.listener.as_fd()), left)?
            {
                return Ok(None);
            }
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    if !peer.ip().is_loopback() {
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                    return Stream::new(stream, self.control).map(Some);
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) =>
                {
                    if timeout.is_some_and(|limit| start.elapsed() >= limit) {
                        return Ok(None);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn bind_script() -> io::Result<(net::SocketAddr, net::TcpListener)> {
    let listener = net::TcpListener::bind((net::Ipv4Addr::LOCALHOST, 0))?;
    let addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    Ok((addr, listener))
}

fn nonempty<const N: usize>() {
    const { assert!(N != 0, "TCP script requires at least one peer") }
}

fn cancelled() -> io::Error {
    io::ErrorKind::Interrupted.into()
}

fn fixed<'a, T, const N: usize>(
    mut next: impl FnMut() -> io::Result<Stream<'a>>,
    script: impl FnOnce(&mut [&Stream<'a>; N]) -> T,
) -> io::Result<T> {
    let mut backing = mem::MaybeUninit::uninit();
    let mut guard = init::Guard::<_, N, true>::new(&mut backing);
    let streams = loop {
        let vacant = match guard.next() {
            ops::ControlFlow::Break(full) => break full.into_array(),
            ops::ControlFlow::Continue(vacant) => vacant,
        };
        guard = vacant.insert(next()?);
    };
    Ok(script(&mut streams.each_ref()))
}
