use std::{convert, io, iter};

use crate::platform;

#[repr(transparent)]
pub(crate) struct Cpu(convert::Infallible);
pub(crate) struct Cpus;

impl platform::Bound for Cpu {
    fn bind(_: u16) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "dope: hard CPU affinity is unavailable on this target",
        ))
    }

    fn cpu(&self) -> u16 {
        match self.0 {}
    }
}

impl platform::Available for Cpus {
    fn current() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "dope: CPU affinity discovery is unavailable on this target",
        ))
    }
}

impl Iterator for Cpus {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl DoubleEndedIterator for Cpus {
    fn next_back(&mut self) -> Option<Self::Item> {
        None
    }
}

impl ExactSizeIterator for Cpus {
    fn len(&self) -> usize {
        0
    }
}

impl iter::FusedIterator for Cpus {}
