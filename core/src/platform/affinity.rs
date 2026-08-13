use std::{io, iter};

use crate::{
    backend,
    platform::{self, Available as _, Bound as _},
};

type Selected = <backend::Backend as platform::Affinity>::Cpus;
type Proof = <backend::Backend as platform::Affinity>::Binding;

/// Result of binding the calling thread to one logical CPU.
///
/// Thread confinement remains with the runtime that owns the thread.
#[doc(hidden)]
#[repr(transparent)]
pub struct Binding(Proof);

/// Allocation-free snapshot of the logical CPUs available to this thread.
pub struct Cpus(Selected);

impl Binding {
    pub fn bind(cpu: u16) -> io::Result<Self> {
        Proof::bind(cpu).map(Self)
    }

    pub fn cpu(&self) -> u16 {
        self.0.cpu()
    }
}

const _: () = {
    assert!(std::mem::size_of::<Binding>() == std::mem::size_of::<Proof>());
    assert!(std::mem::align_of::<Binding>() == std::mem::align_of::<Proof>());
};

impl Cpus {
    pub fn current() -> io::Result<Self> {
        Selected::current().map(Self)
    }
}

impl Iterator for Cpus {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl DoubleEndedIterator for Cpus {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

impl ExactSizeIterator for Cpus {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl iter::FusedIterator for Cpus {}
