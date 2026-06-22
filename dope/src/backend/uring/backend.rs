use std::io;

use super::{Config, Driver, system};

#[derive(Clone, Copy, Debug, Default)]
pub struct Backend;

impl crate::backend::Backend for Backend {
    type Driver = Driver;
    type Config = Config;

    fn new_driver(cfg: Self::Config) -> std::io::Result<Self::Driver> {
        Driver::new(cfg)
    }

    fn init_process(cfg: &Config) -> io::Result<()> {
        // SAFETY: SIGPIPE/SIG_IGN is always safe to set; thread-per-core model has no async-signal concerns here.
        unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
        // SAFETY: mlockall is process-wide best-effort; rc ignored (EPERM/ENOMEM acceptable on capped RLIMIT_MEMLOCK).
        unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) };

        system::Snapshot::raise_nofile()?;
        let snap = system::Snapshot::detect()?;
        snap.check(cfg).map_err(io::Error::from)
    }

    fn init_thread(cpu_id: u16) -> io::Result<()> {
        if cpu_id >= libc::CPU_SETSIZE as u16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: cpu index exceeds CPU_SETSIZE",
            ));
        }
        // SAFETY: cpu_set_t is a plain bitset POD; an all-zero value is a valid empty set.
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        // SAFETY: set is a live cpu_set_t; cpu < CPU_SETSIZE is checked above.
        unsafe {
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(cpu_id as usize, &mut set);
        }
        // SAFETY: set is a live cpu_set_t and the size argument matches its layout.
        let rc = unsafe {
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set)
        };
        if rc != 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn allowed_cpus() -> Vec<u16> {
        // SAFETY: cpu_set_t is a plain bitset POD; an all-zero value is a valid empty set.
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        // SAFETY: set is a live cpu_set_t and the size argument matches its layout.
        let rc = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) };
        if rc != 0 {
            let n = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            return (0..n as u16).collect();
        }
        let mut cpus = Vec::new();
        for cpu in 0..libc::CPU_SETSIZE as usize {
            // SAFETY: set is a live cpu_set_t; cpu < CPU_SETSIZE.
            if unsafe { libc::CPU_ISSET(cpu, &set) } {
                cpus.push(cpu as u16);
            }
        }
        cpus
    }
}
