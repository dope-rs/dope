mod borrow;
mod complete;
mod context;
mod datagram;
mod execute;
mod lower;
mod source;

use std::os::fd;

pub use borrow::Borrow;
pub use complete::Complete;
pub use context::Context;
pub use datagram::Datagram;
pub use execute::Execute;
pub use lower::Lower;
pub use source::Source;

use crate::io::file;

/// File requests retaining their descriptor, path, options, and input borrows.
pub trait Request<'a>: Sized {
    type Buffer;
    type Options;

    fn options(flags: file::Open, mode: file::Mode) -> Self::Options;
    fn open(dir: fd::BorrowedFd<'a>, path: &'a std::ffi::CStr, options: &'a Self::Options) -> Self;
    fn write(file: fd::BorrowedFd<'a>, input: Self::Buffer, offset: file::Offset) -> Self;
    fn sync(file: fd::BorrowedFd<'a>, mode: file::Sync) -> Self;
    fn rename(dir: fd::BorrowedFd<'a>, old: &'a std::ffi::CStr, new: &'a std::ffi::CStr) -> Self;
    fn unlink(dir: fd::BorrowedFd<'a>, name: &'a std::ffi::CStr) -> Self;
}
