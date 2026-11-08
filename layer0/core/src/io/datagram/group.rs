use std::{cell, marker, mem, net, os::fd};

use dope_sys::io::socket::{
    Domain, Kind, Prepare, Socket as _,
    route::{self, Attach, Source as _, count},
};
use o3::alloc::VecExt as _;

use crate::{
    Backend,
    io::{
        datagram::{Error, Lease, Operation},
        socket,
    },
};

/// A completely installed dense UDP routing group.
pub struct Group {
    count: count::Count,
    sockets: Vec<socket::Handle>,
    local: net::SocketAddr,
    ready: <Backend as route::Source>::Ready,
    thread: marker::PhantomData<cell::Cell<()>>,
}

impl Group {
    pub fn bind(addr: net::SocketAddr, plan: route::Plan) -> Result<Self, Error> {
        let mut storage = Backend::STORAGE;
        let (mut install, ready) =
            Backend::try_prepare(&mut storage, plan).map_err(|plan| Error::Unsupported {
                address: addr,
                plan,
            })?;
        let count = plan.count();
        let first = Self::try_open(addr)?;
        install
            .try_attach(fd::AsFd::as_fd(first.owned()))
            .map_err(Error::install)?;
        install
            .try_filter(count::Index::ZERO, fd::AsFd::as_fd(first.owned()))
            .map_err(Error::install)?;
        let failed = |errno| Error::Syscall {
            operation: Operation::Bind,
            errno,
        };
        first.bind(&socket::Inet::from_std(addr)).map_err(failed)?;
        let local = first.local_addr().map_err(Error::LocalAddress)?;
        let addr = local.into_std().map_err(Error::Address)?;
        let bound = socket::Inet::from_std(addr);
        let mut sockets = Vec::try_new(usize::from(count.encode())).map_err(Error::Allocation)?;
        sockets.push(first);
        for index in count.indices().skip(1) {
            let socket = Self::try_open(addr)?;
            install
                .try_filter(index, fd::AsFd::as_fd(socket.owned()))
                .map_err(Error::install)?;
            socket.bind(&bound).map_err(failed)?;
            sockets.push(socket);
        }
        Ok(Self {
            count,
            sockets,
            local: addr,
            ready,
            thread: marker::PhantomData,
        })
    }

    fn try_open(addr: net::SocketAddr) -> Result<socket::Handle, Error> {
        let file = <Backend as Prepare>::open(Domain::for_addr(&addr), Kind::Dgram)
            .map_err(|error| Error::Create(socket::Create::from_native(error)))?;
        let handle = socket::Handle::new(file);
        Backend::set_reuse::<crate::Errno>(
            fd::AsFd::as_fd(handle.owned()),
            dope_sys::io::socket::Reuse::Port,
        )
        .map_err(|errno| Error::Syscall {
            operation: Operation::ReusePort,
            errno,
        })?;
        Ok(handle)
    }

    pub const fn local_addr(&self) -> net::SocketAddr {
        self.local
    }

    pub fn entries(&mut self) -> impl ExactSizeIterator<Item = (count::Index, Lease<'_>)> {
        let local = &self.local;
        let ready = &self.ready;
        self.count
            .indices()
            .zip(&mut self.sockets)
            .map(move |(index, socket)| {
                (
                    index,
                    Lease {
                        socket: Backend::lease(ready, fd::AsFd::as_fd(socket.owned())),
                        local,
                        thread: marker::PhantomData,
                    },
                )
            })
    }
}

const _: () = {
    assert!(mem::size_of::<Group>() == 64);
    assert!(mem::align_of::<Group>() == mem::align_of::<usize>());
};
