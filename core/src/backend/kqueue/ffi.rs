use std::{io, mem};

pub(in crate::backend::kqueue) struct Entropy([u64; 2]);

impl Entropy {
    pub(in crate::backend::kqueue) fn acquire() -> io::Result<Self> {
        let mut words = mem::MaybeUninit::<[u64; 2]>::uninit();
        loop {
            // SAFETY: words provides writable storage for the exact requested size.
            let result =
                unsafe { libc::getentropy(words.as_mut_ptr().cast(), mem::size_of::<[u64; 2]>()) };
            if result == 0 {
                // SAFETY: getentropy initialized the complete array on success.
                return Ok(Self(unsafe { words.assume_init() }));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    pub(in crate::backend::kqueue) const fn into_words(self) -> [u64; 2] {
        self.0
    }
}
