mod borrow;
mod complete;
mod context;
mod datagram;
mod execute;
mod lower;
mod source;

use std::{mem::MaybeUninit, os::fd};

use o3::buffer::bytes::bounded;

pub use borrow::{Borrowed, Retained};
pub use complete::Complete;
pub use context::Context;
pub use datagram::Datagram;
pub use execute::Execute;
pub use lower::Lower;
pub use source::Source;

use crate::io::file;

/// File requests retaining their descriptor, path, options, and buffer borrows.
pub trait Request<'a>: Sized {
    type Buffer;
    type Options;
    type Metadata;

    fn options(flags: file::Open, mode: file::Mode) -> Self::Options;
    fn open(dir: fd::BorrowedFd<'a>, path: &'a std::ffi::CStr, options: &'a Self::Options) -> Self;
    fn metadata(file: fd::BorrowedFd<'a>, output: &'a mut Self::Metadata) -> Self;
    fn read(
        file: fd::BorrowedFd<'a>,
        output: bounded::Value<&'a mut [MaybeUninit<u8>]>,
        offset: file::Offset,
    ) -> Self;
    fn write(file: fd::BorrowedFd<'a>, input: Self::Buffer, offset: file::Offset) -> Self;
    /// Writes at the owned descriptor's current position, honoring `O_APPEND`.
    fn write_current(file: fd::BorrowedFd<'a>, input: Self::Buffer) -> Self;
    fn sync(file: fd::BorrowedFd<'a>, mode: file::Sync) -> Self;
    fn rename(dir: fd::BorrowedFd<'a>, old: &'a std::ffi::CStr, new: &'a std::ffi::CStr) -> Self;
    fn unlink(dir: fd::BorrowedFd<'a>, name: &'a std::ffi::CStr) -> Self;
}
