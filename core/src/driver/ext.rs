use std::io;

use crate::backend::Backend;
use crate::backend::ops::ext::ExtBackend;

use super::{Config, Driver};

pub trait DriverExt: Sized {
    fn new(cfg: Config) -> io::Result<Self>;
    fn init_thread(cpu_id: u16) -> io::Result<()>;
    fn allowed_cpus() -> io::Result<Vec<u16>>;
}

impl DriverExt for Driver {
    fn new(cfg: Config) -> io::Result<Self> {
        cfg.validate()?;
        let (state, slots) = <Backend as ExtBackend>::create(&cfg)?;
        Driver::from_state(state, slots, cfg.ready_slots, cfg.provided.entries as usize)
    }

    fn init_thread(cpu_id: u16) -> io::Result<()> {
        <Backend as ExtBackend>::init_thread(cpu_id)
    }

    fn allowed_cpus() -> io::Result<Vec<u16>> {
        <Backend as ExtBackend>::allowed_cpus()
    }
}
