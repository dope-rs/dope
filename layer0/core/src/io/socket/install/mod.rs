pub mod adopt;
pub mod receive;
pub mod slot;

mod error;

use std::{mem, net, os::fd};

use dope_sys::io::socket::{Domain, Kind, Origin, Prepare};
pub use error::Error;
use fd::AsFd as _;

use crate::{
    driver,
    io::{
        datagram,
        fd::handles::{Datagram, Descriptor},
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

impl socket::ReuseStep for Operation {
    const ADDRESS: Self = Self::ReuseAddress;
    const PORT: Self = Self::ReusePort;
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
            .bind(&socket::Addr::from_std(addr))
            .map_err(|errno| Error::syscall(Operation::Bind, errno))?;
        if let Some(depth) = depth {
            handle
                .listen(depth.get())
                .map_err(|errno| Error::syscall(Operation::Listen, errno))?;
        }
        self.finish(handle)
    }

    fn adopt(
        self,
        file: fd::OwnedFd,
        kind: Kind,
    ) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        let handle = socket::Handle::new(crate::Backend::adopt(file).map_err(Error::setup)?);
        match kind {
            Kind::Stream => handle.check_stream(),
            Kind::Dgram => handle.check_dgram(),
        }
        .map_err(Error::validation)?;
        self.finish(handle)
    }

    fn finish(self, handle: socket::Handle) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        let local = handle.local_addr().map_err(Error::LocalAddress)?;
        let local = local.into_std().map_err(Error::Address)?;
        self.finish_at(handle, local)
    }

    fn finish_at(
        self,
        handle: socket::Handle,
        local: net::SocketAddr,
    ) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        match self.driver.native().install(self.reservation, handle) {
            Ok(fd) => Ok((fd, local)),
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
        reserve(&mut driver)?.adopt(fd, Kind::Stream)
    }

    /// Installs an owned listener without rechecking its established socket role.
    pub fn adopt_bound(
        mut driver: driver::Context<'context, 'd>,
        listener: socket::Listener,
    ) -> Result<(Descriptor<'d>, net::SocketAddr), Error> {
        let entry = reserve(&mut driver)?;
        let (fd, local) = listener.into_parts();
        let handle = socket::Handle::new(crate::Backend::adopt(fd).map_err(Error::setup)?);
        entry.finish_at(handle, local)
    }

    pub fn adopt_udp(
        mut driver: driver::Context<'context, 'd>,
        fd: fd::OwnedFd,
    ) -> Result<(Datagram<'d>, net::SocketAddr), Error> {
        let (entry, capacity) = reserve_datagram(&mut driver)?;
        let (fd, addr) = entry.adopt(fd, Kind::Dgram)?;
        Ok((Datagram::new(fd, capacity), addr))
    }

    /// Installs a routed datagram lease with its established socket role.
    pub fn adopt_routed(
        mut driver: driver::Context<'context, 'd>,
        socket: datagram::Lease,
    ) -> Result<(Datagram<'d>, net::SocketAddr), Error> {
        let (entry, capacity) = reserve_datagram(&mut driver)?;
        let (fd, addr) = entry.finish(socket.into_handle())?;
        Ok((Datagram::new(fd, capacity), addr))
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
