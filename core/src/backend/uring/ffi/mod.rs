mod enter;
pub(in crate::backend::uring) mod fixed;
pub(in crate::backend::uring) mod pipe;
mod provided;
pub(in crate::backend::uring) mod recvmsg;

use std::{io, iter, mem};

pub(in crate::backend::uring) use enter::RegisteredEnter;
pub(in crate::backend::uring) use provided::{Buffer, CanaryRing, ProvidedRing};

use crate::platform;

pub(in crate::backend::uring) struct Entropy([u64; 2]);

#[repr(transparent)]
pub(crate) struct Cpu(u16);

pub(crate) struct Cpus {
    mask: libc::cpu_set_t,
    front: u16,
    back: u16,
    remaining: u16,
}

impl Entropy {
    pub(in crate::backend::uring) fn acquire() -> io::Result<Self> {
        let mut words = mem::MaybeUninit::<[u64; 2]>::uninit();
        let mut data = words.as_mut_ptr().cast::<u8>();
        let mut len = mem::size_of::<[u64; 2]>();
        while len != 0 {
            // SAFETY: `data..data + len` is the uninitialized suffix of
            // `words`, and getrandom initializes at most that suffix.
            let written = unsafe { libc::getrandom(data.cast(), len, 0) };
            if written < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "kernel entropy returned no bytes",
                ));
            }
            let written = written as usize;
            // SAFETY: the kernel cannot report more bytes than the requested
            // `len`, so this remains within the `words` allocation.
            data = unsafe { data.add(written) };
            len -= written;
        }
        // SAFETY: the loop exits only after getrandom initialized every byte.
        Ok(Self(unsafe { words.assume_init() }))
    }

    pub(in crate::backend::uring) const fn into_words(self) -> [u64; 2] {
        self.0
    }
}

impl platform::Bound for Cpu {
    fn bind(raw: u16) -> io::Result<Self> {
        if raw >= libc::CPU_SETSIZE as u16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: cpu index exceeds CPU_SETSIZE",
            ));
        }
        // SAFETY: zero is the empty set and raw is bounded by CPU_SETSIZE.
        let mut mask: libc::cpu_set_t = unsafe { mem::zeroed() };
        // SAFETY: raw is bounded by CPU_SETSIZE and mask is initialized.
        unsafe { libc::CPU_SET(usize::from(raw), &mut mask) };
        // SAFETY: mask is an initialized cpu_set_t of the exact ABI size.
        let result =
            unsafe { libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mask) };
        if result != 0 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!("failed to pin current thread to CPU {raw}"),
            ));
        }
        Ok(Self(raw))
    }

    fn cpu(&self) -> u16 {
        self.0
    }
}

impl platform::Available for Cpus {
    fn current() -> io::Result<Self> {
        const LIMIT: u16 = libc::CPU_SETSIZE as u16;
        const _: () = assert!((libc::CPU_SETSIZE as usize) <= u16::MAX as usize);

        // SAFETY: zero gives the kernel initialized storage even when the CPU
        // set representation contains padding.
        let mut mask: libc::cpu_set_t = unsafe { mem::zeroed() };
        // SAFETY: mask points to writable storage of the exact size supplied.
        let result =
            unsafe { libc::sched_getaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mut mask) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        let remaining = (0..LIMIT)
            // SAFETY: every index is below CPU_SETSIZE and mask is initialized.
            .filter(|cpu| unsafe { libc::CPU_ISSET(usize::from(*cpu), &mask) })
            .count() as u16;
        Ok(Self {
            mask,
            front: 0,
            back: LIMIT,
            remaining,
        })
    }
}

impl Cpus {
    fn contains(&self, cpu: u16) -> bool {
        // SAFETY: the iterator bounds every index by CPU_SETSIZE and current()
        // initialized the mask.
        unsafe { libc::CPU_ISSET(usize::from(cpu), &self.mask) }
    }
}

impl Iterator for Cpus {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        while self.front < self.back {
            let cpu = self.front;
            self.front += 1;
            if self.contains(cpu) {
                self.remaining -= 1;
                return Some(cpu);
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::from(self.remaining);
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for Cpus {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.front < self.back {
            self.back -= 1;
            let cpu = self.back;
            if self.contains(cpu) {
                self.remaining -= 1;
                return Some(cpu);
            }
        }
        None
    }
}

impl ExactSizeIterator for Cpus {}
impl iter::FusedIterator for Cpus {}
