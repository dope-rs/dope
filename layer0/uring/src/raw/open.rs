use std::os::fd;

use dope_sys::{
    Errno,
    io::{
        fd::Fd,
        file,
        reactor::complete::{Decode, Outcome as _},
    },
};
use o3::base::num::bounded;

use crate::{
    Opened, Uring,
    reactor::cq::{Invalid, Outcome},
};

// SAFETY: nonnegative native open results uniquely transfer their descriptor;
// the private owner delivers or closes it exactly once.
unsafe impl file::raw::Completion for Opened {
    type Outcome = Outcome;
    type Invalid = Invalid;

    unsafe fn open(raw: Self::Outcome) -> Result<Self, Self::Invalid> {
        match raw.decode() {
            Decode::Value(value) => {
                let value: bounded::U32<0, { i32::MAX as u32 }> = value.into();
                // SAFETY: the caller owns this successful open completion exactly once.
                let owned =
                    unsafe { <fd::OwnedFd as fd::FromRawFd>::from_raw_fd(value.get() as i32) };
                Ok(Self(Ok(owned)))
            }
            Decode::Error(code) => Ok(Self(Err(code))),
            Decode::Invalid(invalid) => Err(invalid),
        }
    }

    fn close(file: fd::OwnedFd) -> Result<(), Errno> {
        <Uring as Fd>::close(file)
    }
}
