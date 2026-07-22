use std::io;

use crate::backend::Backend;
use crate::driver::Config;

pub(crate) trait ExtBackend {
    fn create(cfg: &Config) -> io::Result<(Backend, usize)>;
    fn init_thread(cpu_id: u16) -> io::Result<()>;
    fn allowed_cpus() -> io::Result<Vec<u16>>;
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io::{self, Error, ErrorKind};
    use std::mem;

    use super::{Backend, Config, ExtBackend};

    impl ExtBackend for Backend {
        fn create(cfg: &Config) -> io::Result<(Backend, usize)> {
            Backend::new(cfg)
        }

        fn init_thread(cpu_id: u16) -> io::Result<()> {
            if cpu_id >= libc::CPU_SETSIZE as u16 {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "dope: cpu index exceeds CPU_SETSIZE",
                ));
            }
            let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
            unsafe {
                libc::CPU_ZERO(&mut set);
                libc::CPU_SET(cpu_id as usize, &mut set);
            }
            let rc = unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) };
            if rc == 0 {
                Ok(())
            } else {
                Err(Error::last_os_error())
            }
        }

        fn allowed_cpus() -> io::Result<Vec<u16>> {
            let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
            let rc = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) };
            if rc != 0 {
                return Err(Error::last_os_error());
            }
            let mut cpus = Vec::new();
            for cpu in 0..libc::CPU_SETSIZE as usize {
                if unsafe { libc::CPU_ISSET(cpu, &set) } {
                    cpus.push(cpu as u16);
                }
            }
            Ok(cpus)
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use std::io::{self, Error, ErrorKind};

    use super::{Backend, Config, ExtBackend};

    impl ExtBackend for Backend {
        fn create(cfg: &Config) -> io::Result<(Backend, usize)> {
            Backend::new(cfg)
        }

        fn init_thread(cpu_id: u16) -> io::Result<()> {
            let _ = cpu_id;
            Err(Error::new(
                ErrorKind::Unsupported,
                "dope: hard CPU affinity is unavailable on this target",
            ))
        }

        fn allowed_cpus() -> io::Result<Vec<u16>> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "dope: CPU affinity discovery is unavailable on this target",
            ))
        }
    }
}
