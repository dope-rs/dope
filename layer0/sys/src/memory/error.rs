use std::num;

use crate::Errno;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Length {
        element: usize,
        count: usize,
    },
    Alignment {
        element: usize,
        supported: usize,
    },
    Map {
        bytes: num::NonZero<usize>,
        cause: Errno,
    },
    Null {
        bytes: num::NonZero<usize>,
    },
    Populate {
        bytes: num::NonZero<usize>,
        cause: Errno,
    },
    Unmap {
        address: *mut libc::c_void,
        bytes: num::NonZero<usize>,
        cause: Errno,
    },
}
