use std::io;

use super::{Config, Driver};

pub trait DriverExt: Sized {
    fn new(cfg: Config) -> io::Result<Self>;
    fn init_thread(cpu_id: u16) -> io::Result<()>;
    fn allowed_cpus() -> io::Result<Vec<u16>>;
}

cfg_select! {
    target_os = "linux" => {
        use std::io::{Error, ErrorKind};
        use std::mem;

        use crate::backend::uring::driver::Uring;

        impl DriverExt for Driver {
            fn new(cfg: Config) -> io::Result<Self> {
                cfg.validate()?;
                let (state, slots) = Uring::new(&cfg)?;
                Driver::from_state(
                    state,
                    slots,
                    cfg.ready_slots,
                    cfg.provided.entries as usize,
                )
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
                if rc != 0 {
                    Err(Error::last_os_error())
                } else {
                    Ok(())
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
    _ => {
        use std::io::{Error, ErrorKind};

        use crate::backend::kqueue::driver::Kqueue;

        impl DriverExt for Driver {
            fn new(cfg: Config) -> io::Result<Self> {
                cfg.validate()?;
                let (state, slots) = Kqueue::new(&cfg)?;
                Driver::from_state(
                    state,
                    slots,
                    cfg.ready_slots,
                    cfg.provided.entries as usize,
                )
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
}
