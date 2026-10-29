use std::{cell, marker, mem, net, os::fd};

use dope_sys::io::socket::{
    Domain, Kind, Prepare,
    route::{self, Attach, Source as _, count, install},
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
    thread: marker::PhantomData<cell::Cell<()>>,
}

impl Group {
    pub fn bind(addr: net::SocketAddr, plan: route::Plan) -> Result<Self, Error> {
        Self::try_install(Backend::try_prepare(plan), addr, plan)
    }

    fn try_install<I: Attach>(
        prepared: Result<I, install::Failure>,
        addr: net::SocketAddr,
        plan: route::Plan,
    ) -> Result<Self, Error> {
        let mut install = prepared.map_err(|error| match error {
            install::Failure::Unsupported(_) => Error::Unsupported {
                address: addr,
                plan,
            },
            install::Failure::Syscall(error) => Error::install(error),
        })?;
        let count = plan.count();
        let first = Self::try_open(addr)?;
        install
            .try_attach(fd::AsFd::as_fd(first.owned()))
            .map_err(Error::install)?;
        let failed = |errno| Error::Syscall {
            operation: Operation::Bind,
            errno,
        };
        first.bind(&socket::Addr::from_std(addr)).map_err(failed)?;
        let local = first.local_addr().map_err(Error::LocalAddress)?;
        let addr = local.into_std().map_err(Error::Address)?;
        let bound = socket::Addr::from_std(addr);
        let mut sockets = Vec::try_new(usize::from(count.encode())).map_err(Error::Allocation)?;
        sockets.push(first);
        while sockets.len() < usize::from(count.encode()) {
            let socket = Self::try_open(addr)?;
            socket.bind(&bound).map_err(failed)?;
            sockets.push(socket);
        }
        let group = Self {
            count,
            sockets,
            thread: marker::PhantomData,
        };
        for (key, socket) in count.indices().zip(&group.sockets) {
            install = install
                .try_insert(key, fd::AsFd::as_fd(socket.owned()))
                .map_err(Error::install)?;
        }
        Ok(group)
    }

    fn try_open(addr: net::SocketAddr) -> Result<socket::Handle, Error> {
        let file = <Backend as Prepare>::open(Domain::for_addr(&addr), Kind::Dgram)
            .map_err(|error| Error::Create(socket::Create::from_native(error)))?;
        let config = socket::Config {
            reuse: Some(socket::Reuse::Port),
            ..socket::Config::default()
        };
        let handle = socket::Handle::new(file);
        handle
            .try_reuse(&config)
            .map_err(|(reuse, errno)| Error::from((reuse, errno)))?;
        Ok(handle)
    }

    pub fn into_entries(self) -> impl ExactSizeIterator<Item = (count::Index, Lease)> {
        self.count
            .indices()
            .zip(self.sockets)
            .map(|(index, socket)| {
                (
                    index,
                    Lease {
                        socket,
                        thread: marker::PhantomData,
                    },
                )
            })
    }
}

const _: () = {
    assert!(mem::size_of::<Group>() == 32);
    assert!(mem::align_of::<Group>() == mem::align_of::<usize>());
};
