use std::{mem, net, ops, os::fd, time};

use dope_core::{
    Backend, Errno,
    dope_sys::{
        Nonblock,
        io::{
            fd::Fd as _,
            poll,
            socket::{self, Prepare as _, Socket as _, addr},
        },
    },
    io::socket::install::adopt,
};
use dope_runtime::process::{
    failure::bus,
    lifecycle::{Control, Main},
};
use fd::AsFd as _;
use o3::{base::marker::ThreadBound, depot::array::init};
use scripts::Script;

use crate::harness::scenario::run;

mod connect;
mod finish;
mod half;
mod scripts;
mod stream;
pub mod udp;

pub type Client<'scope, T, const N: usize = 1> = TcpScript<'scope, T, N>;
pub use finish::Finish;
pub use half::Half;
pub use stream::Stream;

#[derive(Debug)]
pub enum Exchange {
    Write(Error),
    Read(Error),
}

#[derive(Debug)]
pub enum Spawn {
    Io(Error),
    Control(bus::Create),
    Thread(run::Start),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Syscall(Errno),
    Create(socket::Create),
    Setup(socket::Setup),
    Adopt(adopt::Error),
    Vectors(o3::depot::array::Error),
    Address(addr::Error),
    Local(addr::Local<Errno>),
    ConnectEvents(i16),
    Cancelled,
    TimedOut,
    UnexpectedEof,
    WriteZero,
    ZeroTimeout,
    NonLoopback(net::SocketAddr),
}

impl From<Errno> for Error {
    fn from(error: Errno) -> Self {
        Self::Syscall(error)
    }
}

const IO_TIMEOUT: time::Duration = time::Duration::from_secs(5);
const FINISH_TIMEOUT: time::Duration = time::Duration::from_secs(10);

#[derive(Clone, Copy)]
pub struct Peer {
    addr: net::SocketAddr,
}

impl Peer {
    pub const fn at(addr: net::SocketAddr) -> Self {
        Self { addr }
    }

    pub fn try_connect(self) -> Result<Stream<'static>, Error> {
        connect::open(self.addr, None)
    }

    pub fn try_read(self, timeout: time::Duration) -> Result<Stream<'static>, Error> {
        let stream = self.try_connect()?;
        stream.read_wait(Some(timeout))?;
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
            move |control| fixed(|| connect::open(addr, Some(control)), script),
            |inner| run(TcpScript { inner }),
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

    pub fn try_read_all(stream: &Stream<'_>) -> Result<Vec<u8>, Error> {
        let mut got = Vec::new();
        stream.read_to_end(&mut got)?;
        Ok(got)
    }

    pub fn try_reset(socket: impl fd::AsFd) -> Result<(), Error> {
        Backend::reset_on_close(socket.as_fd())
    }
}

mod raw {
    use std::os::fd;

    use dope_core::{
        Backend, Errno,
        dope_sys::io::socket::{addr, raw::Source as _},
    };

    pub(super) fn accept(
        file: fd::BorrowedFd<'_>,
        address: &mut addr::Storage,
    ) -> Result<fd::OwnedFd, Errno> {
        let (storage, len) = address.output_mut();
        // SAFETY: the initialized address and length retain their exclusive full extents.
        unsafe { Backend::accept(file, std::ptr::from_mut(storage).cast(), len) }
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
        let listener =
            bind((net::Ipv4Addr::LOCALHOST, 0).into(), socket::Kind::Stream).map_err(Spawn::Io)?;
        let addr = local(listener.as_fd()).map_err(Spawn::Io)?;
        Script::with(
            addr,
            move |control| {
                let acceptor = TcpAcceptor {
                    listener,
                    control: Some(control),
                    _thread: ThreadBound::NEW,
                };
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

    /// Borrows the completion signal; call `finish` after the registering driver closes.
    pub fn completion(&self) -> &Main {
        self.inner.completion()
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
        let listener =
            bind((net::Ipv4Addr::LOCALHOST, 0).into(), socket::Kind::Stream).map_err(Spawn::Io)?;
        let addr = local(listener.as_fd()).map_err(Spawn::Io)?;
        Script::with(
            addr,
            move |control| {
                let mut acceptor = TcpAcceptor {
                    listener,
                    control: Some(control),
                    _thread: ThreadBound::NEW,
                };
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

    /// Borrows the completion signal; call `finish` after the registering driver closes.
    pub fn completion(&self) -> &Main {
        self.inner.completion()
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
    listener: fd::OwnedFd,
    control: Option<Control<'a>>,
    _thread: ThreadBound,
}

impl TcpAcceptor<'static> {
    pub fn bind(address: net::SocketAddr) -> Result<Self, Error> {
        Ok(Self {
            listener: bind(address, socket::Kind::Stream)?,
            control: None,
            _thread: ThreadBound::NEW,
        })
    }

    /// Consumes one loopback listener, closing the descriptor if admission fails.
    pub fn try_adopt(listener: fd::OwnedFd) -> Result<Self, Error> {
        Ok(Self {
            listener: adopt(listener, socket::Kind::Stream)?,
            control: None,
            _thread: ThreadBound::NEW,
        })
    }
}

impl<'a> TcpAcceptor<'a> {
    pub fn local_addr(&self) -> Result<net::SocketAddr, Error> {
        local(self.listener.as_fd())
    }

    pub fn accept(&self) -> Result<Stream<'a>, Error> {
        match self.next(None)? {
            Some(stream) => Ok(stream),
            None => unreachable!("accept"),
        }
    }

    pub fn try_accept(&self) -> Result<Option<Stream<'a>>, Error> {
        self.next(Some(time::Duration::ZERO))
    }

    pub fn accept_for(&self, timeout: time::Duration) -> Result<Option<Stream<'a>>, Error> {
        self.next(Some(timeout))
    }

    fn next(&self, timeout: Option<time::Duration>) -> Result<Option<Stream<'a>>, Error> {
        let start = time::Instant::now();
        loop {
            if scripts::ready(
                self.control,
                poll::Entry::readable(self.listener.as_fd()),
                start,
                timeout,
            )?
            .is_none()
            {
                return Ok(None);
            }
            let mut address = addr::Storage::empty();
            match raw::accept(self.listener.as_fd(), &mut address) {
                Ok(stream) => {
                    let peer = address.to_std().map_err(Error::Address)?;
                    if !peer.ip().is_loopback() {
                        return Err(Error::NonLoopback(peer));
                    }
                    let stream = Backend::adopt(stream).map_err(Error::Setup)?;
                    return Stream::new(Backend::into_fd(stream), self.control).map(Some);
                }
                Err(error) if error.is_intr() || error.is_block() => {
                    if timeout.is_some_and(|limit| start.elapsed() >= limit) {
                        return Ok(None);
                    }
                }
                Err(error) => return Err(Error::Syscall(error)),
            }
        }
    }
}

impl fd::AsFd for TcpAcceptor<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.listener.as_fd()
    }
}

fn adopt(file: fd::OwnedFd, kind: socket::Kind) -> Result<fd::OwnedFd, Error> {
    let actual = Backend::kind::<Errno>(file.as_fd())?;
    match kind {
        socket::Kind::Stream => {
            if !actual.is_stream() {
                return Err(Error::Adopt(adopt::Error::ListenerKind {
                    actual: actual.raw(),
                }));
            }
            if !Backend::is_listening::<Errno>(file.as_fd())? {
                return Err(Error::Adopt(adopt::Error::ListenerState { actual: 0 }));
            }
        }
        socket::Kind::Dgram => {
            if !actual.is_datagram() {
                return Err(Error::Adopt(adopt::Error::DatagramKind {
                    actual: actual.raw(),
                }));
            }
        }
    }
    let address = local(file.as_fd())?;
    if !address.ip().is_loopback() {
        return Err(Error::NonLoopback(address));
    }
    let file = Backend::adopt(file).map_err(Error::Setup)?;
    let file = Backend::into_fd(file);
    Backend::update::<Nonblock>(file.as_fd())
        .map_err(socket::Setup::nonblock)
        .map_err(Error::Setup)?;
    Ok(file)
}

fn bind(address: net::SocketAddr, kind: socket::Kind) -> Result<fd::OwnedFd, Error> {
    let socket = Backend::open(socket::Domain::for_addr(&address), kind).map_err(Error::Create)?;
    let socket = Backend::into_fd(socket);
    Backend::update::<Nonblock>(socket.as_fd())
        .map_err(socket::Setup::nonblock)
        .map_err(Error::Setup)?;
    addr::Internet::from_std::<Backend>(address)
        .as_addr()
        .try_bind::<Backend, Errno>(socket.as_fd())?;
    if kind == socket::Kind::Stream {
        Backend::listen::<Errno>(socket.as_fd(), socket::BACKLOG)?;
    }
    Ok(socket)
}

fn local(socket: fd::BorrowedFd<'_>) -> Result<net::SocketAddr, Error> {
    let mut address = addr::Storage::empty();
    addr::Addr::local::<Backend, Errno>(socket, &mut address).map_err(Error::Local)?;
    address.to_std().map_err(Error::Address)
}

fn nonempty<const N: usize>() {
    const { assert!(N != 0, "TCP script requires at least one peer") }
}

fn fixed<'a, T, const N: usize>(
    mut next: impl FnMut() -> Result<Stream<'a>, Error>,
    script: impl FnOnce(&mut [&Stream<'a>; N]) -> T,
) -> Result<T, Error> {
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
