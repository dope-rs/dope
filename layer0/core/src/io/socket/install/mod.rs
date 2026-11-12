pub mod adopt;
pub mod receive;
pub mod slot;

mod error;

use std::{mem, net, os::fd};

use dope_sys::{
    Errno, engine,
    io::socket::{Domain, Kind, Origin, Prepare, Socket as _, addr},
    kernel,
};
pub use error::Error;
use fd::AsFd as _;

use crate::{
    driver,
    io::{
        datagram,
        fd::handles::{Datagram, Descriptor, OwnedSlot},
        socket,
    },
};

/// Operating-system operation that failed while bootstrapping a descriptor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Operation {
    Socket,
    ReuseAddress,
    ReusePort,
    FastOpen,
    Bind,
    Listen,
    Validate,
    FixedFile,
}

pub struct Install<'borrow, 'context, 'd> {
    driver: &'borrow mut driver::Context<'context, 'd>,
    reservation: slot::Reserved<'d>,
}

fn reserve<'context, 'd, 'a>(
    driver: &'a mut driver::Context<'context, 'd>,
) -> Result<Install<'a, 'context, 'd>, Error> {
    Ok(Install {
        reservation: slot::Reserved::try_new(driver.driver_ref()).map_err(Error::Slot)?,
        driver,
    })
}

fn reserve_datagram<'context, 'd, 'a>(
    driver: &'a mut driver::Context<'context, 'd>,
) -> Result<(Install<'a, 'context, 'd>, receive::Capacity<'d>), Error> {
    let capacity = receive::Capacity::try_new(driver.driver_ref()).map_err(Error::Receive)?;
    Ok((reserve(driver)?, capacity))
}

impl<'borrow, 'context, 'd> Install<'borrow, 'context, 'd> {
    fn bind<F>(
        self,
        addr: net::SocketAddr,
        config: &socket::Config,
        depth: Option<socket::Backlog>,
    ) -> Result<(Descriptor<'d>, net::SocketAddr), Error>
    where
        crate::Backend: Prepare<Fast = F>,
    {
        let fast = crate::Backend::fast_open(config.fast_open().map(Into::into))
            .map_err(|error| Error::FastOpen(error.0))?;
        let domain = Domain::for_addr(&addr);
        let handle = if depth.is_some() {
            crate::Backend::listener(domain)
        } else {
            crate::Backend::open(domain, Kind::Dgram)
        }
        .map_err(|error| match error.origin() {
            Origin::Socket(errno) => Error::syscall(Operation::Socket, errno),
            Origin::Setup(error) => Error::setup(error),
        })?;
        let handle = socket::Handle::new(handle);
        handle.try_reuse(config).map_err(Error::reuse)?;
        if let Some(fast) = fast {
            crate::Backend::set_fast(handle.owned().as_fd(), fast)
                .map_err(|errno| Error::syscall(Operation::FastOpen, errno))?;
        }
        handle
            .bind(&socket::Inet::from_std(addr))
            .map_err(|errno| Error::syscall(Operation::Bind, errno))?;
        if let Some(depth) = depth {
            handle
                .listen(depth.get())
                .map_err(|errno| Error::syscall(Operation::Listen, errno))?;
        }
        self.finish(handle)
    }

    fn adopt(file: fd::OwnedFd, kind: Kind) -> Result<socket::Handle, Error> {
        let handle = socket::Handle::new(crate::Backend::adopt(file).map_err(Error::setup)?);
        let actual =
            crate::Backend::kind::<Errno>(handle.owned().as_fd()).map_err(Error::validate)?;
        match kind {
            Kind::Stream if !actual.is_stream() => {
                return Err(Error::Adopt(adopt::Error::StreamKind {
                    actual: actual.raw(),
                }));
            }
            Kind::Dgram if !actual.is_datagram() => {
                return Err(Error::Adopt(adopt::Error::DatagramKind {
                    actual: actual.raw(),
                }));
            }
            Kind::Stream | Kind::Dgram => {}
        }
        Ok(handle)
    }

    fn finish(self, handle: socket::Handle) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        let local = handle.local_addr().map_err(Error::LocalAddress)?;
        let local = local.into_std().map_err(Error::Address)?;
        Ok((self.install(handle.into_inner())?, local))
    }

    fn install<H, E>(self, handle: H) -> Result<Descriptor<'d>, Error>
    where
        crate::Engine: engine::Install<OwnedSlot, H, Error = E>,
        E: Into<<crate::Backend as kernel::Source>::Failure<driver::route::indices::SlotIndex>>,
    {
        match self.driver.native().install(self.reservation, handle) {
            Ok(fd) => Ok(fd),
            Err(cause) => Err(Error::kernel(Operation::FixedFile, cause)),
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Install<'static, 'static, 'static>>()
            == mem::size_of::<usize>() + mem::size_of::<slot::Reserved<'static>>()
    );
    assert!(mem::align_of::<Install<'static, 'static, 'static>>() == mem::align_of::<usize>());
};

impl<'borrow, 'context, 'd> Install<'borrow, 'context, 'd> {
    pub fn bind_stream(
        mut driver: driver::Context<'context, 'd>,
        addr: net::SocketAddr,
        depth: socket::Backlog,
        config: &socket::Config,
    ) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        reserve(&mut driver)?.bind(addr, config, Some(depth))
    }

    pub fn adopt_stream(
        mut driver: driver::Context<'context, 'd>,
        fd: fd::OwnedFd,
    ) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        let entry = reserve(&mut driver)?;
        let handle = Self::adopt(fd, Kind::Stream)?;
        if !crate::Backend::is_listening::<Errno>(handle.owned().as_fd())
            .map_err(Error::validate)?
        {
            return Err(Error::Adopt(adopt::Error::ListenerState { actual: 0 }));
        }
        entry.finish(handle)
    }

    /// Installs an owned connected Internet stream.
    pub fn adopt_connected(
        mut driver: driver::Context<'context, 'd>,
        fd: fd::OwnedFd,
    ) -> Result<Descriptor<'d>, Error> {
        let entry = reserve(&mut driver)?;
        let handle = Self::adopt(fd, Kind::Stream)?;
        let mut peer = addr::Storage::empty();
        crate::Backend::peer_addr::<Errno>(handle.owned().as_fd(), &mut peer)
            .map_err(Error::validate)?;
        peer.to_std()
            .map_err(socket::Error::from)
            .map_err(Error::Address)?;
        entry.install(handle.into_inner())
    }

    /// Installs an owned listener without rechecking its established socket role.
    pub fn adopt_bound(
        mut driver: driver::Context<'context, 'd>,
        listener: socket::Listener,
    ) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        let entry = reserve(&mut driver)?;
        let (fd, local) = listener.into_parts();
        let handle = socket::Handle::new(crate::Backend::adopt(fd).map_err(Error::setup)?);
        Ok((entry.install(handle.into_inner())?, local))
    }

    pub fn adopt_udp(
        mut driver: driver::Context<'context, 'd>,
        fd: fd::OwnedFd,
    ) -> Result<(Datagram<'d>, net::SocketAddr), Error> {
        let (entry, capacity) = reserve_datagram(&mut driver)?;
        let handle = Self::adopt(fd, Kind::Dgram)?;
        let (fd, addr) = entry.finish(handle)?;
        Ok((Datagram::new(fd, capacity), addr))
    }

    /// Installs a routed datagram lease with its established socket role.
    pub fn adopt_routed<'g: 'd>(
        mut driver: driver::Context<'context, 'd>,
        socket: datagram::Lease<'g>,
    ) -> Result<(Datagram<'d>, net::SocketAddr), Error> {
        let (entry, capacity) = reserve_datagram(&mut driver)?;
        let (handle, local) = socket.into_parts();
        let fd = entry.install(handle)?;
        Ok((Datagram::new(fd, capacity), local))
    }

    pub fn bind_udp(
        mut driver: driver::Context<'context, 'd>,
        addr: net::SocketAddr,
        config: socket::Config,
    ) -> Result<(Datagram<'d>, net::SocketAddr), Error> {
        let (entry, capacity) = reserve_datagram(&mut driver)?;
        let (fd, addr) = entry.bind(addr, &config, None)?;
        Ok((Datagram::new(fd, capacity), addr))
    }
}
