use std::os::fd;

use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

use crate::Errno;

/// Consumes completion authority with the exact native file result.
pub trait Complete: Sized {
    type Output;

    fn opened(self, result: Result<fd::OwnedFd, Errno>) -> Self::Output;
    fn write(self, result: Result<bounded::U32<0, { MAX as u32 }>, Errno>) -> Self::Output;
    fn sync(self, result: Result<(), Errno>) -> Self::Output;
}
