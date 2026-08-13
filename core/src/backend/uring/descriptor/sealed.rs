use std::{io, ops, os::fd};

use crate::io::socket;

#[repr(transparent)]
pub(crate) struct Handle(socket::raw::Handle);

const _: () = assert!(size_of::<Handle>() == size_of::<fd::OwnedFd>());
const _: () = assert!(align_of::<Handle>() == align_of::<fd::OwnedFd>());

impl Handle {
    pub(crate) fn blocking(domain: socket::Domain, kind: socket::Kind) -> io::Result<Self> {
        Self::open(domain, kind, 0)
    }

    pub(crate) fn nonblocking(domain: socket::Domain, kind: socket::Kind) -> io::Result<Self> {
        Self::open(domain, kind, libc::SOCK_NONBLOCK)
    }

    fn open(domain: socket::Domain, kind: socket::Kind, flags: libc::c_int) -> io::Result<Self> {
        let socket_type = kind.raw() | libc::SOCK_CLOEXEC | flags;
        // SAFETY: socket takes scalar arguments and returns a fresh descriptor on success.
        let raw = unsafe { libc::socket(domain.raw(), socket_type, 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful socket returned a fresh owned descriptor.
        let fd = unsafe { fd::FromRawFd::from_raw_fd(raw) };
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
