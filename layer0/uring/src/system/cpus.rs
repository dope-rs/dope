use std::{iter, ops};

use dope_sys::thread::affinity;

pub struct Cpus {
    mask: libc::cpu_set_t,
    indices: ops::Range<u16>,
    remaining: u16,
}

impl Cpus {
    pub(crate) const LIMIT: u16 = {
        assert!(libc::CPU_SETSIZE > 0 && (libc::CPU_SETSIZE as usize) <= u16::MAX as usize);
        libc::CPU_SETSIZE as u16
    };

    pub(crate) fn new(mask: &libc::cpu_set_t) -> Self {
        // SAFETY: current() initialized the complete mask. Its population is
        // at most CPU_SETSIZE, whose bound above also proves this narrowing.
        let count = unsafe { libc::CPU_COUNT(mask) } as u16;
        Self {
            mask: *mask,
            indices: 0..Self::LIMIT,
            remaining: count,
        }
    }
}

impl Cpus {
    fn contains(mask: &libc::cpu_set_t, cpu: u16) -> bool {
        // SAFETY: the iterator bounds every index by CPU_SETSIZE and current()
        // initialized the mask.
        unsafe { libc::CPU_ISSET(crate::raw::cpu(cpu).into_usize(), mask) }
    }
}

impl Iterator for Cpus {
    type Item = affinity::raw::Cpu;

    fn next(&mut self) -> Option<Self::Item> {
        let cpu = self.indices.find(|&cpu| Self::contains(&self.mask, cpu))?;
        self.remaining -= 1;
        // SAFETY: the range only yields indices below CPU_SETSIZE.
        Some(unsafe { affinity::raw::Cpu::new(cpu) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = usize::from(self.remaining);
        (len, Some(len))
    }
}

impl DoubleEndedIterator for Cpus {
    fn next_back(&mut self) -> Option<Self::Item> {
        let cpu = self.indices.rfind(|&cpu| Self::contains(&self.mask, cpu))?;
        self.remaining -= 1;
        // SAFETY: the range only yields indices below CPU_SETSIZE.
        Some(unsafe { affinity::raw::Cpu::new(cpu) })
    }
}

impl ExactSizeIterator for Cpus {}
impl iter::FusedIterator for Cpus {}
