use std::{
    io,
    os::{fd, fd::AsRawFd as _},
};

use dope_core::Backend;
use dope_sys::io::socket::Socket as _;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Socket<'fd>(fd::BorrowedFd<'fd>);

impl<'fd> Socket<'fd> {
    pub const fn new(fd: fd::BorrowedFd<'fd>) -> Self {
        Self(fd)
    }

    pub fn try_cloexec(self) -> io::Result<bool> {
        // SAFETY: fcntl reads descriptor flags from a live borrowed socket.
        let flags = unsafe { libc::fcntl(self.0.as_raw_fd(), libc::F_GETFD) };
        if flags < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(flags & libc::FD_CLOEXEC != 0)
        }
    }

    pub fn send(self, bytes: &[u8]) -> io::Result<usize> {
        let kind = Backend::get_option::<{ libc::SO_TYPE }, io::Error>(self.0)?;
        if kind != libc::SOCK_STREAM {
            return Backend::send_quiet::<io::Error>(self.0, bytes).map(<[u8]>::len);
        }
        let length = u8::try_from(bytes.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let mut frame = Vec::with_capacity(bytes.len() + 1);
        frame.push(length);
        frame.extend_from_slice(bytes);
        let mut pending = frame.as_slice();
        while !pending.is_empty() {
            match Backend::send_quiet::<io::Error>(self.0, pending) {
                Ok([]) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(sent) => pending = &pending[sent.len()..],
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(bytes.len())
    }
}
