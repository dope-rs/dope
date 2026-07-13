use std::io;
use std::os::fd::RawFd;

use dope::platform::Pipe;
use dope::sqe::Sqe;
use dope::{Drive, Driver};

pub struct Trigger {
    pipe: Pipe,
}

impl Trigger {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            pipe: Pipe::shutdown()?,
        })
    }

    pub fn register(&self, driver: &Driver) -> bool {
        driver.push(Sqe::poll_shutdown(self.pipe.read_fd())).is_ok()
    }

    pub fn fire(&self) {
        self.pipe.fire();
    }
}

pub struct SignalTrigger {
    fd: RawFd,
}

impl SignalTrigger {
    pub fn new() -> io::Result<Self> {
        Ok(Self { fd: Self::open()? })
    }

    pub fn register(&self, driver: &Driver) -> bool {
        driver.push(Sqe::poll_shutdown(self.fd)).is_ok()
    }
}

cfg_select! {
    target_os = "linux" => {
        impl SignalTrigger {
            fn open() -> io::Result<RawFd> {
                // SAFETY: zeroed sigset_t is valid; signalfd/pthread_sigmask are standard one-time setup.
                let fd = unsafe {
                    let mut set: libc::sigset_t = std::mem::zeroed();
                    libc::sigemptyset(&mut set);
                    libc::sigaddset(&mut set, libc::SIGINT);
                    libc::sigaddset(&mut set, libc::SIGTERM);
                    libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
                    libc::signalfd(-1, &set, libc::SFD_CLOEXEC)
                };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(fd)
            }
        }
    }
    _ => {
        impl SignalTrigger {
            fn open() -> io::Result<RawFd> {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "signalfd is linux-only",
                ))
            }
        }
    }
}
