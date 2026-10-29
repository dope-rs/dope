use std::os::fd::{AsFd as _, OwnedFd};

use dope_sys::{
    Cloexec, Errno, Nonblock,
    io::{
        fd::Fd as _,
        socket::{self, Create, Domain, Kind, Setup, Socket as _, accept},
    },
};
use libc::socklen_t;

use crate::{Kqueue, sockets::Handle};

impl socket::raw::Open for Handle {
    /// The fresh socket has no mutable status flags to preserve.
    fn open(domain: Domain, kind: Kind) -> Result<Self, Create> {
        let file =
            Kqueue::try_open::<Errno>(domain.raw(), kind.raw(), 0).map_err(Create::socket)?;
        Kqueue::cloexec(file.as_fd())
            .map_err(Setup::cloexec)
            .map_err(Create::setup)?;
        Kqueue::set_retry::<{ libc::F_SETFL }, Errno>(file.as_fd(), libc::O_NONBLOCK)
            .map_err(Nonblock::Set)
            .map_err(Setup::nonblock)
            .map_err(Create::setup)?;
        Handle::prepare(file).map_err(Create::setup)
    }

    fn adopt(file: OwnedFd) -> Result<Self, Setup> {
        Kqueue::update::<Cloexec>(file.as_fd()).map_err(Setup::cloexec)?;
        Kqueue::update::<Nonblock>(file.as_fd()).map_err(Setup::nonblock)?;
        Handle::prepare(file)
    }

    unsafe fn accept(
        &self,
        addr: *mut libc::sockaddr_storage,
        len: *mut socklen_t,
    ) -> Result<Self, accept::Error> {
        if !len.is_null() {
            unsafe { *len = size_of::<libc::sockaddr_storage>() as socklen_t };
        }
        let file = unsafe {
            <Kqueue as socket::raw::Source>::accept::<Errno>(self.as_fd(), addr.cast(), len)
        }
        .map_err(accept::Error::Syscall)?;
        Kqueue::cloexec(file.as_fd()).map_err(accept::Error::Cloexec)?;
        Kqueue::set_retry::<{ libc::F_SETFL }, Errno>(file.as_fd(), libc::O_NONBLOCK)
            .map_err(Nonblock::Set)
            .map_err(Setup::nonblock)
            .map_err(accept::Error::Setup)?;
        Handle::prepare(file).map_err(accept::Error::Setup)
    }
}
