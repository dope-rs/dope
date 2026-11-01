use std::{cell, io, mem, ops};

use dope_core::platform::{affinity, affinity::Cpu};
use o3::depot::array::init;

pub fn try_limit() -> io::Result<(libc::rlim_t, libc::rlim_t)> {
    let mut limit = mem::MaybeUninit::uninit();
    // SAFETY: the output points to storage for one complete resource limit.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful query initialized both limit fields.
    let limit = unsafe { limit.assume_init() };
    Ok((limit.rlim_cur, limit.rlim_max))
}

pub trait Cpus: Sized {
    fn try_current() -> Result<Self, Error>;
}

impl<const N: usize> Cpus for [Cpu; N] {
    fn try_current() -> Result<Self, Error> {
        let mut cpus = Cpu::current().map_err(Error::Discovery)?;
        let actual = cpus.len();
        let mut output = mem::MaybeUninit::uninit();
        let mut guard = init::Guard::<_, N>::new(&mut output);
        loop {
            let vacant = match guard.next() {
                ops::ControlFlow::Break(full) => return Ok(full.into_array()),
                ops::ControlFlow::Continue(vacant) => vacant,
            };
            let cpu = cpus.next().ok_or(Error::Count {
                actual,
                required: N,
            })?;
            guard = vacant.insert(cpu);
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Discovery(affinity::Error),
    Count { actual: usize, required: usize },
}

pub struct Resources {
    pub values: ops::Range<usize>,
    pub declared: usize,
}

impl Iterator for Resources {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        self.values.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.declared, Some(self.declared))
    }
}

impl ExactSizeIterator for Resources {}

pub struct Input<'a>(pub &'a cell::Cell<Option<usize>>);

impl Drop for Input<'_> {
    fn drop(&mut self) {
        self.0
            .set(self.0.get().and_then(|drops| drops.checked_add(1)));
    }
}
