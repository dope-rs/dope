use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

pub struct Pipe {
    read: OwnedFd,
    write: OwnedFd,
}

impl Pipe {
    pub fn shutdown() -> io::Result<Self> {
        let mut fds = [0 as RawFd; 2];
        cfg_select! {
            target_os = "linux" => {
                // SAFETY: `fds` is a valid 2-element array; pipe2 fills both descriptors on success.
                let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
                if rc != 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            _ => {
                // SAFETY: `fds` is a valid 2-element array; pipe fills both descriptors on success.
                let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
                if rc != 0 {
                    return Err(io::Error::last_os_error());
                }
                for fd in fds {
                    // SAFETY: fd was just returned by pipe and is live.
                    unsafe {
                        libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
                        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
                        if flags >= 0 {
                            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
                        }
                    }
                }
            }
        }
        Ok(Self::from_fds(fds[0], fds[1]))
    }

    pub(super) fn from_fds(read: RawFd, write: RawFd) -> Self {
        // SAFETY: fds freshly returned by pipe(2); ownership transfers here, drop closes both.
        unsafe {
            Self {
                read: OwnedFd::from_raw_fd(read),
                write: OwnedFd::from_raw_fd(write),
            }
        }
    }

    pub fn read_fd(&self) -> RawFd {
        self.read.as_raw_fd()
    }

    pub fn fire(&self) {
        let byte = 1u8;
        // SAFETY: writes one byte from a live stack address to the pipe write end; never blocks on an empty pipe.
        unsafe { libc::write(self.write.as_raw_fd(), (&byte as *const u8).cast(), 1) };
    }
}
