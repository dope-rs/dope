use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::ptr::NonNull;

use dope_core::backend::Sqe;
use dope_core::driver::token::Token;
use dope_core::io::fd::FdSlot;
use o3::buffer::Block;

pub(super) struct ReadRegion {
    ptr: NonNull<MaybeUninit<u8>>,
    len: u32,
}

pub(super) struct ReadSubmission {
    sqe: Sqe,
    region: ReadRegion,
}

impl ReadRegion {
    pub(super) fn vec(buffer: &mut Vec<u8>) -> Option<Self> {
        let len = buffer.len().min(u32::MAX as usize) as u32;
        if len == 0 {
            return None;
        }
        Some(Self {
            ptr: unsafe { NonNull::new_unchecked(buffer.as_mut_ptr().cast()) },
            len,
        })
    }

    pub(super) fn block(buffer: &mut Block) -> Option<Self> {
        let mut writer = buffer.spare_writer();
        let len = writer.remaining() as u32;
        if len == 0 {
            return None;
        }
        Some(Self {
            ptr: unsafe { NonNull::new_unchecked(writer.as_mut_ptr().cast()) },
            len,
        })
    }

    pub(super) fn direct(self, fd: RawFd, offset: u64, token: Token) -> ReadSubmission {
        let region = self;
        let span =
            unsafe { std::slice::from_raw_parts_mut(region.ptr.as_ptr(), region.len as usize) };
        let sqe = unsafe { Sqe::read_uninit(fd, span, offset, token) };
        ReadSubmission { sqe, region }
    }

    pub(super) fn fixed(self, fd: FdSlot, offset: u64, token: Token) -> ReadSubmission {
        let region = self;
        let span =
            unsafe { std::slice::from_raw_parts_mut(region.ptr.as_ptr(), region.len as usize) };
        let sqe = Sqe::read_fixed_file_uninit(fd, span, offset, token);
        ReadSubmission { sqe, region }
    }

    pub(super) fn commit_vec(self, buffer: &mut Vec<u8>, amount: u32) -> io::Result<()> {
        self.validate(amount)?;
        if buffer.as_mut_ptr().cast() != self.ptr.as_ptr() || buffer.len() < self.len as usize {
            return Err(Self::changed());
        }
        Ok(())
    }

    pub(super) fn commit_block(self, buffer: &mut Block, amount: u32) -> io::Result<()> {
        self.validate(amount)?;
        let mut writer = buffer.spare_writer();
        if writer.as_mut_ptr().cast() != self.ptr.as_ptr() || writer.remaining() < self.len as usize
        {
            return Err(Self::changed());
        }
        let initialized =
            unsafe { std::slice::from_raw_parts(writer.as_mut_ptr(), amount as usize) };
        writer
            .try_commit_initialized(initialized)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn validate(&self, amount: u32) -> io::Result<()> {
        if amount > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope::file: completion exceeded its prepared read region",
            ));
        }
        Ok(())
    }

    fn changed() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "dope::file: prepared read buffer changed before completion",
        )
    }
}

impl ReadSubmission {
    pub(super) fn into_parts(self) -> (Sqe, ReadRegion) {
        (self.sqe, self.region)
    }
}
