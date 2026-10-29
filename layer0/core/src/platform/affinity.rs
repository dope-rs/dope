use std::iter;

use dope_sys::thread::{Thread as _, affinity};
use o3::base::marker::ThreadBound;

use crate::{Backend, Errno};

/// Result of binding the calling thread to one logical CPU.
///
/// The receipt is confined to the thread that performed the binding. A later
/// affinity change can replace that binding.
#[repr(transparent)]
pub struct Binding {
    cpu: Cpu,
    thread: ThreadBound,
}

impl Binding {
    pub fn bind(cpu: Cpu) -> Result<Self, Error> {
        Backend::bind(cpu.0).map_err(Error::from_native)?;
        Ok(Self {
            cpu,
            thread: ThreadBound::NEW,
        })
    }

    pub fn cpu(&self) -> Cpu {
        self.cpu
    }
}

const _: () = {
    assert!(std::mem::size_of::<Binding>() == std::mem::size_of::<Cpu>());
    assert!(std::mem::align_of::<Binding>() == std::mem::align_of::<Cpu>());
};

/// A CPU proven present in a native affinity discovery result.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Cpu(affinity::raw::Cpu);

impl Cpu {
    pub fn current() -> Result<
        impl DoubleEndedIterator<Item = Self> + ExactSizeIterator + iter::FusedIterator,
        Error,
    > {
        Backend::current()
            .map(|cpus| cpus.map(Self))
            .map_err(Error::from_native)
    }
    pub const fn raw(self) -> u16 {
        self.0.raw()
    }
    pub const fn matches(self, index: Index) -> bool {
        self.raw() == index.0
    }
}

impl From<Cpu> for u64 {
    fn from(cpu: Cpu) -> Self {
        u64::from(cpu.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Index(u16);

impl From<u16> for Index {
    fn from(index: u16) -> Self {
        Self(index)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    DiscoveryUnsupported,
    BindingUnsupported { cpu: Cpu },
    BindingRange { cpu: Cpu, maximum: u16 },
    Current(Errno),
    Bind { cpu: Cpu, fault: Errno },
}

impl Error {
    fn from_native(error: affinity::Error) -> Self {
        match error {
            affinity::Error::DiscoveryUnsupported => Self::DiscoveryUnsupported,
            affinity::Error::BindingUnsupported { cpu } => {
                Self::BindingUnsupported { cpu: Cpu(cpu) }
            }
            affinity::Error::BindingRange { cpu, maximum } => Self::BindingRange {
                cpu: Cpu(cpu),
                maximum,
            },
            affinity::Error::Current(error) => Self::Current(error),
            affinity::Error::Bind { cpu, fault } => Self::Bind {
                cpu: Cpu(cpu),
                fault,
            },
        }
    }
}
