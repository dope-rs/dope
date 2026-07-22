use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::ptr::NonNull;

use dope_core::backend::Sqe;
use dope_core::driver::token::Token;

pub(super) struct ReadRegion {
    ptr: NonNull<MaybeUninit<u8>>,
    len: u32,
}

impl ReadRegion {
    pub(super) fn new(buffer: &mut Vec<u8>) -> Option<Self> {
        let len = buffer.len().min(u32::MAX as usize) as u32;
        (len != 0).then(|| Self {
            ptr: unsafe { NonNull::new_unchecked(buffer.as_mut_ptr().cast()) },
            len,
        })
    }

    pub(super) fn submission(self, fd: RawFd, offset: u64, token: Token) -> (Sqe, Self) {
        let span = unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len as usize) };
        let sqe = unsafe { Sqe::read_uninit(fd, span, offset, token) };
        (sqe, self)
    }

    pub(super) fn commit(self, buffer: &mut Vec<u8>, amount: u32) -> io::Result<()> {
        if amount > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope::file: completion exceeded its prepared read region",
            ));
        }
        if buffer.as_mut_ptr().cast() != self.ptr.as_ptr() || buffer.len() < self.len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope::file: prepared read buffer changed before completion",
            ));
        }
        Ok(())
    }
}
