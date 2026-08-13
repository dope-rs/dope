use std::{io, os::fd, ptr};

use crate::backend::kqueue::{self, descriptor};

pub(in crate::backend::kqueue) struct Setup(fd::OwnedFd);

impl Setup {
    pub(in crate::backend::kqueue) fn open() -> io::Result<Self> {
        // SAFETY: `kqueue` has no pointer arguments and returns a fresh descriptor on success.
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful `kqueue` returned a fresh owned descriptor.
        let kq = unsafe { fd::FromRawFd::from_raw_fd(raw) };
        descriptor::Init::new(fd::AsFd::as_fd(&kq)).close_on_exec()?;
        let wake = libc::kevent {
            ident: kqueue::WAKE_IDENT,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        };
        // SAFETY: all pointers describe live inputs; this registration requests no output.
        let rc = unsafe {
            libc::kevent(
                fd::AsRawFd::as_raw_fd(&kq),
                &wake,
                1,
                ptr::null_mut(),
                0,
                ptr::null(),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self(kq))
    }

    pub(in crate::backend::kqueue) fn into_fd(self) -> fd::OwnedFd {
        self.0
    }
}

pub(in crate::backend::kqueue) struct Pipe {
    read: fd::OwnedFd,
    write: fd::OwnedFd,
}

impl Pipe {
    pub(in crate::backend::kqueue) fn open() -> io::Result<Self> {
        let mut fds = [0; 2];
        // SAFETY: `fds` is writable storage for exactly two descriptors.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        let [read, write] = fds;
        // SAFETY: successful `pipe` returned a fresh owned descriptor pair.
        let read = unsafe { fd::FromRawFd::from_raw_fd(read) };
        // SAFETY: successful `pipe` returned a fresh owned descriptor pair.
        let write = unsafe { fd::FromRawFd::from_raw_fd(write) };
        let read_init = descriptor::Init::new(fd::AsFd::as_fd(&read));
        read_init.close_on_exec()?;
        let write_init = descriptor::Init::new(fd::AsFd::as_fd(&write));
        write_init.close_on_exec()?;
        Ok(Self { read, write })
    }

    pub(in crate::backend::kqueue) fn open_nonblocking() -> io::Result<Self> {
        let pipe = Self::open()?;
        descriptor::Init::new(fd::AsFd::as_fd(&pipe.read)).nonblocking()?;
        descriptor::Init::new(fd::AsFd::as_fd(&pipe.write)).nonblocking()?;
        Ok(pipe)
    }

    pub(in crate::backend::kqueue) fn into_ends(self) -> (fd::OwnedFd, fd::OwnedFd) {
        (self.read, self.write)
    }
}
