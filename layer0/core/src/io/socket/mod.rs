use std::os::fd::{self, AsFd as _};

pub use addr::{
    Addr,
    inet::{Inet, Listener},
    prepared::Prepared,
    unix::Unix,
};
pub use backlog::Backlog;
use dope_sys::{
    Cloexec, Errno, Nonblock,
    io::{
        socket,
        socket::{SetupOrigin, Socket as _, addr::Local},
    },
};
pub use error::Error;
use install::adopt;

use crate::{
    Backend, driver,
    driver::{
        life::native::{
            request,
            request::{bound, permits},
        },
        result,
        retained::flight,
        route,
        route::kind,
    },
    io::fd::handles,
};

mod addr;
pub(crate) mod adoption;
mod backlog;
mod error;

pub mod install;
pub mod msg;
pub mod option;
pub mod pending;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct StreamSpec(socket::Domain);

impl StreamSpec {
    pub(crate) const fn domain(self) -> socket::Domain {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Reuse {
    Address,
    Port,
}

/// One bootstrap error vocabulary that can name a reuse step.
pub(crate) trait ReuseStep: Sized {
    /// Step attributed when `SO_REUSEADDR` fails.
    const ADDRESS: Self;
    /// Step attributed when `SO_REUSEPORT` fails.
    const PORT: Self;
}

/// Exact bootstrap step this admitted reuse option performs.
pub(crate) const fn reuse_step<S: ReuseStep>(reuse: Reuse) -> S {
    match reuse {
        Reuse::Address => S::ADDRESS,
        Reuse::Port => S::PORT,
    }
}

#[derive(Clone, Copy)]
pub struct Config {
    pub reuse: Option<Reuse>,
    pub(crate) fast_open: Option<Backlog>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            reuse: Some(Reuse::Address),
            fast_open: None,
        }
    }
}

impl Config {
    pub const fn for_datagram(reuse: Option<Reuse>) -> Self {
        Self {
            reuse,
            fast_open: None,
        }
    }

    /// Builds the reuse and fast-open policy for a TCP listener.
    /// `Some(backlog)` requires Fast Open before binding and fails creation
    /// when the platform does not support it.
    pub fn for_tcp(reuse: Option<Reuse>, fast: Option<Backlog>) -> Self {
        Self {
            reuse,
            fast_open: fast,
        }
    }

    pub(crate) const fn fast_open(&self) -> Option<Backlog> {
        self.fast_open
    }

    pub(crate) const fn has_addr(&self) -> bool {
        matches!(self.reuse, Some(Reuse::Address | Reuse::Port))
    }

    pub(crate) const fn has_port(&self) -> bool {
        matches!(self.reuse, Some(Reuse::Port))
    }
}

const _: () = assert!(std::mem::size_of::<Option<Reuse>>() == std::mem::size_of::<bool>());

#[repr(transparent)]
pub(crate) struct Handle {
    fd: <Backend as socket::Prepare>::Handle,
}

impl Handle {
    pub(crate) const fn new(fd: <Backend as socket::Prepare>::Handle) -> Self {
        Self { fd }
    }

    pub(crate) const fn owned(&self) -> &<Backend as socket::Prepare>::Handle {
        &self.fd
    }

    pub(crate) fn into_inner(self) -> <Backend as socket::Prepare>::Handle {
        self.fd
    }

    pub(crate) fn check_stream(&self) -> Result<(), adoption::Error> {
        let kind = Backend::kind::<Errno>(self.fd.as_fd()).map_err(adoption::Error::Syscall)?;
        if !kind.is_stream() {
            return Err(adoption::Error::Role(adopt::Error::ListenerKind {
                actual: kind.raw(),
            }));
        }
        if !Backend::is_listening::<Errno>(self.fd.as_fd()).map_err(adoption::Error::Syscall)? {
            return Err(adoption::Error::Role(adopt::Error::ListenerState {
                actual: 0,
            }));
        }
        Ok(())
    }

    pub(crate) fn check_dgram(&self) -> Result<(), adoption::Error> {
        let kind = Backend::kind::<Errno>(self.fd.as_fd()).map_err(adoption::Error::Syscall)?;
        if !kind.is_datagram() {
            return Err(adoption::Error::Role(adopt::Error::DatagramKind {
                actual: kind.raw(),
            }));
        }
        Ok(())
    }

    pub(crate) fn bind(&self, addr: &Addr) -> Result<(), Errno> {
        addr.storage()
            .as_addr()
            .try_bind::<Backend, Errno>(self.fd.as_fd())
    }

    pub(crate) fn listen(&self, backlog: i32) -> Result<(), Errno> {
        Backend::listen(self.fd.as_fd(), backlog)
    }

    pub(crate) fn try_reuse(&self, config: &Config) -> Result<(), (Reuse, Errno)> {
        if config.has_addr() {
            Backend::set_reuse::<Errno>(self.fd.as_fd(), socket::Reuse::Address)
                .map_err(|error| (Reuse::Address, error))?;
        }
        if !config.has_port() {
            return Ok(());
        }
        Backend::set_reuse::<Errno>(self.fd.as_fd(), socket::Reuse::Port)
            .map_err(|error| (Reuse::Port, error))
    }

    pub(crate) fn local_addr(&self) -> Result<Addr, Local<Errno>> {
        Addr::local(self.fd.as_fd())
    }
}

const _: () = {
    assert!(size_of::<Handle>() == size_of::<fd::OwnedFd>());
    assert!(align_of::<Handle>() == align_of::<fd::OwnedFd>());
    assert!(size_of::<Result<(), Errno>>() == size_of::<i32>());
    assert!(size_of::<Result<(), (Reuse, Errno)>>() == 2 * size_of::<i32>());
};

pub struct Open<'a, 'd: 'a, ID: route::Tag> {
    raw: request::Lowered,
    slot: handles::SocketSlot<'d>,
    flight: flight::Vacant<'a, 'd, ID, kind::Socket>,
    permit: permits::Admission<'a, 'd>,
}

impl<'a, 'd, ID: route::Tag + route::Select<kind::Socket>> Open<'a, 'd, ID>
where
    'd: 'a,
{
    /// A raw descriptor cannot replace the routed socket owner.
    pub fn try_new(
        driver: &'a mut driver::Context<'_, 'd>,
        slots: &'a flight::Slots<'d, ID>,
        req: route::Bound<'d, ID, handles::SocketSlot<'d>>,
        socket: StreamSpec,
    ) -> Result<Self, result::SubmitError>
    where
        'd: 'a,
    {
        let (target, slot) = req.into_parts();
        let raw = request::Lowered::socket(slot.outbound(), socket);
        let flight = slots
            .try_vacancy(target.operation::<kind::Socket>())
            .map_err(result::SubmitError::from)?;
        let permit = driver
            .native()
            .try_admit()
            .map_err(result::SubmitError::queue)?;
        Ok(Open {
            raw,
            slot,
            flight,
            permit,
        })
    }

    pub fn commit(self) -> handles::Creation<'d> {
        let Self {
            raw,
            slot,
            flight,
            permit,
        } = self;
        let entry = bound::Bound::scoped(raw, flight);
        let flight = permit.submit(entry);
        slot.begin(flight)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum BindError {
    Socket(crate::Errno),
    DescriptorFlags(crate::Errno),
    CloseOnExec(crate::Errno),
    Bind(crate::Errno),
    Listen(crate::Errno),
    LocalAddress(Local<Errno>),
    Address(Error),
}

impl BindError {
    fn socket(error: impl Into<socket::Failure>) -> Self {
        match error.into() {
            socket::Failure::Socket(error) => Self::Socket(error),
            socket::Failure::Cloexec(Cloexec::Get(error)) => Self::DescriptorFlags(error),
            socket::Failure::Cloexec(Cloexec::Set(error)) => Self::CloseOnExec(error),
        }
    }
}

/// The exact step that failed while creating a socket descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CreationStep {
    Socket,
    DescriptorFlags,
    CloseOnExec,
    StatusFlags,
    Nonblocking,
    Sigpipe,
}

impl CreationStep {
    /// Exact step and native fault of one failed descriptor creation.
    pub(crate) const fn from_origin(origin: socket::Origin) -> (Self, crate::Errno) {
        match origin {
            socket::Origin::Socket(fault) => (Self::Socket, fault),
            socket::Origin::Setup(setup) => match setup.origin() {
                SetupOrigin::Cloexec(Cloexec::Get(error)) => (Self::DescriptorFlags, error),
                SetupOrigin::Cloexec(Cloexec::Set(error)) => (Self::CloseOnExec, error),
                SetupOrigin::Nonblock(Nonblock::Get(error)) => (Self::StatusFlags, error),
                SetupOrigin::Nonblock(Nonblock::Set(error)) => (Self::Nonblocking, error),
                SetupOrigin::Sigpipe(error) => (Self::Sigpipe, error),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Create {
    fault: crate::Errno,
    step: CreationStep,
}

impl Create {
    pub const fn errno(self) -> crate::Errno {
        self.fault
    }
    pub const fn step(self) -> CreationStep {
        self.step
    }

    pub(crate) fn from_native(error: socket::Create) -> Self {
        let (step, fault) = CreationStep::from_origin(error.origin());
        Self { step, fault }
    }
}
