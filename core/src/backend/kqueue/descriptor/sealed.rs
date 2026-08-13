use std::{io, mem, ops, os::fd, ptr};

use crate::io::socket;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::backend::kqueue) struct Init<'fd>(fd::BorrowedFd<'fd>);

impl<'fd> Init<'fd> {
    pub(in crate::backend::kqueue) const fn new(fd: fd::BorrowedFd<'fd>) -> Self {
        Self(fd)
    }

    pub(in crate::backend::kqueue) fn close_on_exec(self) -> io::Result<()> {
        let result = unsafe {
            libc::fcntl(
                fd::AsRawFd::as_raw_fd(&self.0),
                libc::F_SETFD,
                libc::FD_CLOEXEC,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(in crate::backend::kqueue) fn nonblocking(self) -> io::Result<()> {
        let result = unsafe {
            libc::fcntl(
                fd::AsRawFd::as_raw_fd(&self.0),
                libc::F_SETFL,
                libc::O_NONBLOCK,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::backend::kqueue) struct Options<'fd>(fd::BorrowedFd<'fd>);

const _: () = {
    assert!(mem::size_of::<Init<'static>>() == mem::size_of::<fd::BorrowedFd<'static>>());
    assert!(mem::align_of::<Init<'static>>() == mem::align_of::<fd::BorrowedFd<'static>>());
    assert!(mem::size_of::<Options<'static>>() == mem::size_of::<fd::BorrowedFd<'static>>());
    assert!(mem::align_of::<Options<'static>>() == mem::align_of::<fd::BorrowedFd<'static>>());
};

impl<'fd> Options<'fd> {
    pub(in crate::backend::kqueue) const fn new(fd: fd::BorrowedFd<'fd>) -> Self {
        Self(fd)
    }

    pub(in crate::backend::kqueue) fn no_sigpipe(self) -> io::Result<()> {
        self.set(libc::SOL_SOCKET, libc::SO_NOSIGPIPE, &1)
            .map_err(|()| io::Error::last_os_error())
    }

    pub(in crate::backend::kqueue) fn set(
        self,
        level: libc::c_int,
        name: libc::c_int,
        value: &libc::c_int,
    ) -> Result<(), ()> {
        let result = unsafe {
            libc::setsockopt(
                fd::AsRawFd::as_raw_fd(&self.0),
                level,
                name,
                ptr::from_ref(value).cast(),
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if result == 0 { Ok(()) } else { Err(()) }
    }
}

#[repr(transparent)]
pub(crate) struct Handle(socket::raw::Handle);

const _: () = assert!(size_of::<Handle>() == size_of::<fd::OwnedFd>());
const _: () = assert!(align_of::<Handle>() == align_of::<fd::OwnedFd>());

impl Handle {
    pub(crate) fn open(domain: socket::Domain, kind: socket::Kind) -> io::Result<Self> {
        // SAFETY: socket takes scalar arguments and returns a fresh descriptor on success.
        let raw = unsafe { libc::socket(domain.raw(), kind.raw(), 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful socket returned a fresh owned descriptor.
        let fd: fd::OwnedFd = unsafe { fd::FromRawFd::from_raw_fd(raw) };
        let borrowed = fd::AsFd::as_fd(&fd);
        let init = Init::new(borrowed);
        init.close_on_exec()?;
        init.nonblocking()?;
        Options::new(borrowed).no_sigpipe()?;
        Ok(Self(socket::raw::Handle::from_owned(fd)))
    }

    pub(crate) fn from_inheriting_accept(fd: fd::OwnedFd) -> io::Result<Self> {
        Init::new(fd::AsFd::as_fd(&fd)).close_on_exec()?;
        Ok(Self(socket::raw::Handle::from_owned(fd)))
    }
}

impl ops::Deref for Handle {
    type Target = socket::raw::Handle;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Handle> for fd::OwnedFd {
    fn from(handle: Handle) -> Self {
        handle.0.into()
    }
}
