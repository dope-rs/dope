use std::os::{fd, fd::AsRawFd as _};

use dope_core::{Backend, Errno};
use dope_sys::{io::socket::Socket as _, kernel};

#[derive(Debug)]
pub enum Error {
    Syscall(Errno),
    Length { actual: usize },
    WriteZero,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Socket<'fd>(fd::BorrowedFd<'fd>);

impl<'fd> Socket<'fd> {
    pub const fn new(fd: fd::BorrowedFd<'fd>) -> Self {
        Self(fd)
    }

    pub fn try_cloexec(self) -> Result<bool, Errno> {
        // SAFETY: fcntl reads descriptor flags from a live borrowed socket.
        let flags = unsafe { libc::fcntl(self.0.as_raw_fd(), libc::F_GETFD) };
        // SAFETY: fcntl returns nonnegative flags or -1 with this thread's errno.
        let flags = unsafe {
            kernel::raw::Status::new(flags)
                .try_value::<Errno>(|| kernel::raw::Code::last::<Backend>().into())
        }?;
        Ok(flags.get() & libc::FD_CLOEXEC != 0)
    }

    pub fn send(self, bytes: &[u8]) -> Result<usize, Error> {
        let kind =
            Backend::get_option::<{ libc::SO_TYPE }, Errno>(self.0).map_err(Error::Syscall)?;
        if kind != libc::SOCK_STREAM {
            return Backend::send_quiet::<Errno>(self.0, bytes)
                .map(<[u8]>::len)
                .map_err(Error::Syscall);
        }
        let length = u8::try_from(bytes.len()).map_err(|_| Error::Length {
            actual: bytes.len(),
        })?;
        let mut frame = Vec::with_capacity(bytes.len() + 1);
        frame.push(length);
        frame.extend_from_slice(bytes);
        let mut pending = frame.as_slice();
        while !pending.is_empty() {
            match Backend::send_quiet::<Errno>(self.0, pending) {
                Ok([]) => return Err(Error::WriteZero),
                Ok(sent) => pending = &pending[sent.len()..],
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(Error::Syscall(error)),
            }
        }
        Ok(bytes.len())
    }
}
