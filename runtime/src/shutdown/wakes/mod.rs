use std::{io, mem, os::fd};

use dope_core::platform::wake;

mod sealed;

pub(super) use sealed::Descriptor;

#[repr(transparent)]
pub(crate) struct Wait(fd::OwnedFd);

#[repr(transparent)]
pub(crate) struct Notify(fd::OwnedFd);

pub(crate) struct Ends<R> {
    read: R,
    notify: Notify,
}

impl Ends<Wait> {
    pub(crate) fn blocking() -> io::Result<Self> {
        let (read, write) = wake::Pair::blocking()?.split();
        Ok(Self {
            read: Wait(read),
            notify: Notify(write),
        })
    }
}

impl Ends<fd::OwnedFd> {
    pub(crate) fn event() -> io::Result<Self> {
        let (read, write) = wake::Pair::nonblocking()?.split();
        Ok(Self {
            read,
            notify: Notify(write),
        })
    }
}

impl<R> Ends<R> {
    pub(crate) fn split(self) -> (R, Notify) {
        (self.read, self.notify)
    }
}

impl Wait {
    pub(crate) fn wait(self) -> io::Result<()> {
        let descriptor = Descriptor::new(fd::AsFd::as_fd(&self.0));
        let mut byte = 0u8;
        loop {
            match descriptor.read(&mut byte) {
                1 => return Ok(()),
                -1 => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
                _ => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            }
        }
    }
}

impl Notify {
    pub(crate) fn notify(self) -> io::Result<()> {
        let descriptor = Descriptor::new(fd::AsFd::as_fd(&self.0));
        let byte = 1u8;
        loop {
            match descriptor.write(&byte) {
                1 => return Ok(()),
                -1 => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
                _ => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            }
        }
    }

    pub(crate) fn fork(self) -> io::Result<(Self, Self)> {
        let guard = self.0.try_clone()?;
        Ok((Self(guard), self))
    }
}

const _: () = {
    assert!(mem::size_of::<Wait>() == mem::size_of::<fd::OwnedFd>());
    assert!(mem::align_of::<Wait>() == mem::align_of::<fd::OwnedFd>());
    assert!(mem::size_of::<Notify>() == mem::size_of::<fd::OwnedFd>());
    assert!(mem::align_of::<Notify>() == mem::align_of::<fd::OwnedFd>());
};
