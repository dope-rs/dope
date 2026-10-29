use std::num::NonZero;

/// Exact unmap failure; the native cleanup state retains mapping ownership.
#[derive(Debug, Eq, PartialEq)]
pub struct Failure<E> {
    address: *mut libc::c_void,
    len: NonZero<usize>,
    code: E,
}

impl<E> Failure<E> {
    pub const fn new(address: *mut libc::c_void, len: NonZero<usize>, code: E) -> Self {
        Self { address, len, code }
    }

    pub fn into_parts(self) -> (*mut libc::c_void, NonZero<usize>, E) {
        (self.address, self.len, self.code)
    }
}
