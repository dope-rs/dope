use std::{mem::MaybeUninit, os::fd};

use crate::{
    Errno,
    io::file::{self, Opened},
    kernel,
};

pub(super) fn read_at<'a, S: kernel::raw::Source + ?Sized, E: From<Errno>>(
    file: fd::BorrowedFd<'_>,
    output: &'a mut [MaybeUninit<u8>],
    offset: file::Offset,
) -> Result<&'a mut [u8], E> {
    use fd::AsRawFd as _;

    const { assert!(libc::off_t::MAX as u64 == i64::MAX as u64) };
    // SAFETY: the descriptor stays borrowed, output owns its writable extent,
    // and the admitted nonnegative offset fits the ABI.
    let raw = unsafe {
        libc::pread(
            file.as_raw_fd(),
            output.as_mut_ptr().cast(),
            output.len(),
            offset.get() as libc::off_t,
        )
    };
    // SAFETY: pread returns -1 or initializes at most this exact extent.
    Ok(unsafe {
        crate::io::raw::Buffer::try_native(output, raw, || kernel::raw::Code::last::<S>().into())?
            .assume_init_mut()
    })
}

/// # Safety
/// The live descriptor remains unchanged until stream drop.
/// Returned names retain exclusive storage until their borrows end.
pub unsafe trait Directory: kernel::raw::Source {
    type Stream;

    fn open_dir<E: From<Errno>>(path: &std::ffi::CStr) -> Result<Self::Stream, file::OpenError<E>>
    where
        Self: file::File,
    {
        let file = Self::open(
            path,
            file::Open::DIRECTORY | file::Open::CLOSE_ON_EXEC,
            file::Mode::new(0),
        )
        .map_err(file::OpenError::Syscall)?;
        Self::from_fd(file)
    }

    fn from_fd<E: From<Errno>>(file: fd::OwnedFd) -> Result<Self::Stream, file::OpenError<E>>;
    fn descriptor(stream: &Self::Stream) -> kernel::raw::Descriptor;
    fn next<E: From<Errno>>(
        stream: &mut Self::Stream,
    ) -> Result<Option<file::Entry<'_>>, file::ReadError<E>>;
}

/// # Safety
/// Each decoded success retains the uniquely transferred descriptor until
/// delivery or reclamation. Closing consumes that ownership.
pub unsafe trait Completion {
    type Outcome;
    type Invalid;

    /// # Safety
    /// A successful outcome transfers one live descriptor from a completed
    /// open operation. Its descriptor has not been decoded or reclaimed before.
    unsafe fn open(outcome: Self::Outcome) -> Result<Opened, Self::Invalid>;

    fn close(file: fd::OwnedFd) -> Result<(), Errno>;
}
