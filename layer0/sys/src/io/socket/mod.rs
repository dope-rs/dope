use std::{
    mem, net, num,
    os::fd::{AsFd, AsRawFd as _, BorrowedFd, OwnedFd},
    ptr,
};

use libc::socklen_t;
use option::fast;

use crate::{
    Cloexec, Errno, Nonblock,
    io::{self, fd},
    kernel::{self, errno},
};

pub mod accept;
pub mod addr;
pub mod gso;
pub mod msg;
pub mod option;
pub mod raw;
pub mod record;
pub mod route;

/// Exact backlog bound the host admits for one listening socket.
pub const BACKLOG: i32 = libc::SOMAXCONN;

/// Exact synchronous connect failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Connect(Errno);

impl Connect {
    pub const fn errno(self) -> Errno {
        self.0
    }
}

impl From<Errno> for Connect {
    fn from(errno: Errno) -> Self {
        Self(errno)
    }
}

const _: () = assert!(size_of::<Connect>() == size_of::<Errno>());

/// Exact failure to create and initialize one socket descriptor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct Create {
    fault: Errno,
    kind: Option<SetupKind>,
}

/// Concrete leaf retained by one compact creation failure.
pub enum Origin {
    Socket(Errno),
    Setup(Setup),
}

impl Create {
    /// Records failure of the descriptor-creation syscall itself.
    pub const fn socket(fault: Errno) -> Self {
        Self { fault, kind: None }
    }

    /// Records the exact descriptor setup operation that failed after creation.
    pub const fn setup(error: Setup) -> Self {
        Self {
            fault: error.fault,
            kind: Some(error.kind),
        }
    }

    pub const fn origin(self) -> Origin {
        match self.kind {
            Some(kind) => Origin::Setup(Setup {
                fault: self.fault,
                kind,
            }),
            None => Origin::Socket(self.fault),
        }
    }
}

const _: () = assert!(mem::size_of::<Create>() == 2 * mem::size_of::<u32>());

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Domain {
    Inet4,
    Inet6,
    Unix,
}

impl Domain {
    pub fn for_addr(addr: &net::SocketAddr) -> Self {
        if addr.is_ipv4() {
            Self::Inet4
        } else {
            Self::Inet6
        }
    }

    pub const fn raw(self) -> libc::c_int {
        use libc::{AF_INET, AF_INET6};
        match self {
            Self::Inet4 => AF_INET,
            Self::Inet6 => AF_INET6,
            Self::Unix => libc::AF_UNIX,
        }
    }
}

/// One observed `SO_TYPE` value of an adopted descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Type(libc::c_int);

impl Type {
    /// Whether this descriptor is one connected byte stream.
    pub const fn is_stream(self) -> bool {
        self.0 == libc::SOCK_STREAM
    }

    /// Whether this descriptor is one datagram socket.
    pub const fn is_datagram(self) -> bool {
        self.0 == libc::SOCK_DGRAM
    }

    /// Raw observed value, for exact diagnostics.
    pub const fn raw(self) -> i32 {
        self.0
    }
}

/// One half-close direction for [`Socket::shutdown`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct How(libc::c_int);

impl How {
    pub const READ: Self = Self(libc::SHUT_RD);
    pub const WRITE: Self = Self(libc::SHUT_WR);
    pub const BOTH: Self = Self(libc::SHUT_RDWR);

    const fn raw(self) -> libc::c_int {
        self.0
    }
}

/// One requested address reuse admitted before binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reuse {
    Address,
    Port,
}

/// Exact failure while opening a close-on-exec socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Socket(Errno),
    Cloexec(Cloexec),
}

impl From<Errno> for Failure {
    fn from(error: Errno) -> Self {
        Self::Socket(error)
    }
}

impl From<Cloexec> for Failure {
    fn from(error: Cloexec) -> Self {
        Self::Cloexec(error)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Stream,
    Dgram,
}

impl Kind {
    pub const fn raw(self) -> libc::c_int {
        use libc::{SOCK_DGRAM, SOCK_STREAM};
        match self {
            Self::Stream => SOCK_STREAM,
            Self::Dgram => SOCK_DGRAM,
        }
    }
}

/// Native socket ownership and fast-open support before driver installation.
pub trait Prepare: Socket {
    type Handle: AsFd;
    type Fast;

    fn fast_open(depth: Option<num::NonZero<i32>>) -> Result<Option<Self::Fast>, fast::Error>;
    fn open(domain: Domain, kind: Kind) -> Result<Self::Handle, Create>;
    fn listener(domain: Domain) -> Result<Self::Handle, Create>;
    fn adopt(file: OwnedFd) -> Result<Self::Handle, Setup>;
    /// Transfers the prepared socket's descriptor without changing its native configuration.
    fn into_fd(handle: Self::Handle) -> OwnedFd;
    fn set_fast(file: BorrowedFd<'_>, fast: Self::Fast) -> Result<(), Errno>;
}

/// Exact socket descriptor setup failure.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct Setup {
    fault: Errno,
    kind: SetupKind,
}

/// Concrete leaf retained by one compact setup failure.
pub enum SetupOrigin {
    Cloexec(Cloexec),
    Nonblock(Nonblock),
    Sigpipe(Errno),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum SetupKind {
    GetFd,
    SetFd,
    GetFlags,
    SetFlags,
    Sigpipe,
}

impl Setup {
    pub const fn cloexec(error: Cloexec) -> Self {
        match error {
            Cloexec::Get(fault) => Self {
                fault,
                kind: SetupKind::GetFd,
            },
            Cloexec::Set(fault) => Self {
                fault,
                kind: SetupKind::SetFd,
            },
        }
    }

    pub const fn nonblock(error: Nonblock) -> Self {
        match error {
            Nonblock::Get(fault) => Self {
                fault,
                kind: SetupKind::GetFlags,
            },
            Nonblock::Set(fault) => Self {
                fault,
                kind: SetupKind::SetFlags,
            },
        }
    }

    pub const fn sigpipe(fault: Errno) -> Self {
        Self {
            fault,
            kind: SetupKind::Sigpipe,
        }
    }

    pub const fn origin(self) -> SetupOrigin {
        match self.kind {
            SetupKind::GetFd => SetupOrigin::Cloexec(Cloexec::Get(self.fault)),
            SetupKind::SetFd => SetupOrigin::Cloexec(Cloexec::Set(self.fault)),
            SetupKind::GetFlags => SetupOrigin::Nonblock(Nonblock::Get(self.fault)),
            SetupKind::SetFlags => SetupOrigin::Nonblock(Nonblock::Set(self.fault)),
            SetupKind::Sigpipe => SetupOrigin::Sigpipe(self.fault),
        }
    }
}

const _: () = assert!(mem::size_of::<Setup>() == 2 * mem::size_of::<u32>());

/// Native socket creation and asynchronous tuning capability.
pub trait Socket: errno::Source {
    const MAX_IOVECS: usize;
    type Error;
    type Records: record::Transport;

    fn socket(domain: libc::c_int, kind: libc::c_int) -> Result<OwnedFd, Self::Error>;
    /// Creates exclusively owned endpoints using this backend's record transport.
    fn pair() -> Result<(OwnedFd, OwnedFd), record::Error>;

    /// Creates one datagram descriptor without changing its descriptor flags.
    fn datagram<E: From<Errno>>(domain: Domain) -> Result<OwnedFd, E> {
        let protocol = match domain {
            Domain::Inet4 | Domain::Inet6 => libc::IPPROTO_UDP,
            Domain::Unix => 0,
        };
        Self::try_open(domain.raw(), libc::SOCK_DGRAM, protocol)
    }

    fn try_open<E: From<Errno>>(
        domain: libc::c_int,
        kind: libc::c_int,
        protocol: libc::c_int,
    ) -> Result<OwnedFd, E> {
        let raw = unsafe { libc::socket(domain, kind, protocol) };
        unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt(raw, || kernel::raw::Code::last::<Self>().into())
        }
    }

    fn listen<E: From<Errno>>(file: BorrowedFd<'_>, backlog: libc::c_int) -> Result<(), E> {
        // SAFETY: listen returns exactly zero or minus one; errno is read immediately.
        unsafe {
            kernel::raw::Status::new(libc::listen(file.as_raw_fd(), backlog))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    /// Half-closes one socket in the requested direction.
    fn shutdown<E: From<Errno>>(file: BorrowedFd<'_>, how: How) -> Result<(), E> {
        // SAFETY: shutdown returns exactly zero or minus one; errno is read immediately.
        unsafe {
            kernel::raw::Status::new(libc::shutdown(file.as_raw_fd(), how.raw()))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    /// Observes the socket type of one adopted descriptor.
    fn kind<E: From<Errno>>(file: BorrowedFd<'_>) -> Result<Type, E> {
        Self::get_option::<{ libc::SO_TYPE }, E>(file).map(Type)
    }

    /// Whether one adopted descriptor is already listening.
    fn is_listening<E: From<Errno>>(file: BorrowedFd<'_>) -> Result<bool, E> {
        Ok(Self::get_option::<{ libc::SO_ACCEPTCONN }, E>(file)? != 0)
    }

    /// Admits the requested address reuse on one socket.
    fn set_reuse<E: From<Errno>>(file: BorrowedFd<'_>, reuse: Reuse) -> Result<(), E> {
        let option = match reuse {
            Reuse::Address => libc::SO_REUSEADDR,
            Reuse::Port => libc::SO_REUSEPORT,
        };
        Self::set_option(file, libc::SOL_SOCKET, option, &1)
    }

    /// Sends one byte string without raising the termination signal.
    fn send_quiet<'a, E: From<Errno>>(
        file: BorrowedFd<'_>,
        input: &'a [u8],
    ) -> Result<&'a [u8], E> {
        Self::send::<{ libc::MSG_NOSIGNAL }, E>(file, input)
    }

    /// Sends without waiting or raising the termination signal.
    fn send_now<'a, E: From<Errno>>(file: BorrowedFd<'_>, input: &'a [u8]) -> Result<&'a [u8], E> {
        Self::send::<{ libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL }, E>(file, input)
    }

    fn get_option<const OPTION: libc::c_int, E: From<Errno>>(
        file: BorrowedFd<'_>,
    ) -> Result<libc::c_int, E> {
        const {
            assert!(
                OPTION == libc::SO_TYPE
                    || OPTION == libc::SO_ACCEPTCONN
                    || OPTION == libc::SO_ERROR
            );
        }
        let mut value = 0;
        let mut len = mem::size_of::<libc::c_int>() as socklen_t;
        let raw = unsafe {
            libc::getsockopt(
                file.as_raw_fd(),
                libc::SOL_SOCKET,
                OPTION,
                ptr::from_mut(&mut value).cast(),
                &mut len,
            )
        };
        // SAFETY: getsockopt returns exactly zero or minus one.
        unsafe {
            kernel::raw::Status::new(raw).try_zero::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?;
        Ok(value)
    }

    fn set_option<E: From<Errno>>(
        file: BorrowedFd<'_>,
        level: libc::c_int,
        option: libc::c_int,
        value: &libc::c_int,
    ) -> Result<(), E> {
        let raw = unsafe {
            libc::setsockopt(
                file.as_raw_fd(),
                level,
                option,
                ptr::from_ref(value).cast(),
                mem::size_of::<libc::c_int>() as socklen_t,
            )
        };
        // SAFETY: setsockopt returns exactly zero or minus one.
        unsafe {
            kernel::raw::Status::new(raw).try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    fn send<'a, const FLAGS: libc::c_int, E: From<Errno>>(
        file: BorrowedFd<'_>,
        input: &'a [u8],
    ) -> Result<&'a [u8], E> {
        const { assert!(FLAGS & !(libc::MSG_NOSIGNAL | libc::MSG_DONTWAIT) == 0) };
        let raw =
            unsafe { libc::send(file.as_raw_fd(), input.as_ptr().cast(), input.len(), FLAGS) };
        // SAFETY: send returns minus one or a count within the supplied input.
        unsafe {
            io::raw::Buffer::try_native(input, raw, || kernel::raw::Code::last::<Self>().into())
        }
    }

    fn recv<'a, const FLAGS: libc::c_int, E: From<Errno>>(
        file: BorrowedFd<'_>,
        output: &'a mut [u8],
    ) -> Result<&'a mut [u8], E> {
        const { assert!(FLAGS & !(libc::MSG_DONTWAIT | libc::MSG_PEEK | libc::MSG_WAITALL) == 0) };
        let raw = unsafe {
            libc::recv(
                file.as_raw_fd(),
                output.as_mut_ptr().cast(),
                output.len(),
                FLAGS,
            )
        };
        // SAFETY: the admitted flags exclude MSG_TRUNC, so recv cannot report a
        // successful count beyond the supplied output capacity.
        unsafe {
            io::raw::Buffer::try_native(output, raw, || kernel::raw::Code::last::<Self>().into())
        }
    }

    fn try_pair<E: From<Errno>>(kind: libc::c_int) -> Result<(OwnedFd, OwnedFd), E> {
        let mut fds = [-1; 2];
        let raw = unsafe { libc::socketpair(libc::AF_UNIX, kind, 0, fds.as_mut_ptr()) };
        // SAFETY: socketpair just returned on this thread and `fds` is its exact destination.
        unsafe {
            fd::raw::Pair::new(fds).try_adopt::<E>(raw, || kernel::raw::Code::last::<Self>().into())
        }
    }
}
