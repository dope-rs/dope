use std::os::fd;

use dope_sys::io::{
    file,
    reactor::complete::{Decode, Outcome as _},
};

use crate::{
    Uring,
    reactor::cq::{Invalid, Outcome},
};

// SAFETY: nonnegative native open results uniquely transfer their descriptor;
// the returned OwnedFd delivers or closes it exactly once.
unsafe impl file::raw::Completion for Uring {
    type Outcome = Outcome;
    type Invalid = Invalid;

    unsafe fn open(raw: Self::Outcome) -> Result<file::Opened, Self::Invalid> {
        match raw.decode() {
            Decode::Value(value) => {
                // SAFETY: the caller owns this successful open completion exactly once.
                let owned =
                    unsafe { <fd::OwnedFd as fd::FromRawFd>::from_raw_fd(value.get() as i32) };
                Ok(Ok(owned))
            }
            Decode::Error(code) => Ok(Err(code)),
            Decode::Invalid(invalid) => Err(invalid),
        }
    }
}
