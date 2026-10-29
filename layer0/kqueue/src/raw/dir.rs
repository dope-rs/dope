use std::{
    num,
    os::fd::{AsRawFd as _, IntoRawFd as _, OwnedFd},
    ptr, slice,
};

use dope_sys::{Errno, io::file, kernel};

use crate::Kqueue;

#[derive(Debug)]
pub struct Dir(ptr::NonNull<libc::DIR>);

// SAFETY: fdopendir owns one stable directory descriptor on success; the native
// stream retains each returned name until the next read or its exclusive drop.
unsafe impl file::raw::Directory for Kqueue {
    type Stream = Dir;

    fn open_dir<E: From<Errno>>(path: &std::ffi::CStr) -> Result<Dir, file::OpenError<E>>
    where
        Self: file::File,
    {
        match ptr::NonNull::new(unsafe { libc::opendir(path.as_ptr()) }) {
            Some(stream) => Ok(Dir(stream)),
            // SAFETY: opendir failed and left this thread's errno positive.
            None => Err(file::OpenError::Syscall(E::from(Errno::from(unsafe {
                kernel::raw::Code::last::<Self>()
            })))),
        }
    }

    fn from_fd<E: From<Errno>>(file: OwnedFd) -> Result<Dir, file::OpenError<E>> {
        match ptr::NonNull::new(unsafe { libc::fdopendir(file.as_raw_fd()) }) {
            Some(stream) => {
                let _ = file.into_raw_fd();
                Ok(Dir(stream))
            }
            // SAFETY: fdopendir failed without taking ownership and left errno
            // positive; the original OwnedFd closes only after this read.
            None => Err(file::OpenError::Syscall(E::from(Errno::from(unsafe {
                kernel::raw::Code::last::<Self>()
            })))),
        }
    }

    fn descriptor(stream: &Dir) -> kernel::raw::Descriptor {
        // SAFETY: the uniquely owned native stream retains its live nonnegative descriptor.
        unsafe { kernel::raw::Descriptor::new(libc::dirfd(stream.0.as_ptr())) }
    }

    fn next<E: From<Errno>>(
        stream: &mut Dir,
    ) -> Result<Option<file::Entry<'_>>, file::ReadError<E>> {
        // SAFETY: this slot belongs to the calling thread for this synchronous read.
        let errno = unsafe { libc::__error() };
        unsafe { errno.write(0) };
        match ptr::NonNull::new(unsafe { libc::readdir(stream.0.as_ptr()) }) {
            Some(entry) => {
                // SAFETY: readdir retains d_namlen name bytes and their trailing
                // NUL through stream's exclusive borrow. Project raw fields
                // because records need not occupy a full libc::dirent value.
                let name = unsafe {
                    let len = usize::from(ptr::addr_of!((*entry.as_ptr()).d_namlen).read()) + 1;
                    let bytes = ptr::addr_of!((*entry.as_ptr()).d_name).cast();
                    std::ffi::CStr::from_bytes_with_nul_unchecked(slice::from_raw_parts(bytes, len))
                };
                let kind = unsafe { ptr::addr_of!((*entry.as_ptr()).d_type).read() };
                Ok(Some(file::Entry {
                    name,
                    kind: file::Kind::from_dirent(kind),
                }))
            }
            None => match num::NonZero::new(unsafe { errno.read() })
                .map(Errno::try_from)
                .transpose()
                .map_err(file::ReadError::Invalid)?
            {
                Some(error) => Err(file::ReadError::Syscall(E::from(error))),
                None => Ok(None),
            },
        }
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        // SAFETY: this exclusive owner releases the native stream exactly once.
        // Drop cannot report close errors; retrying could close a reused descriptor.
        let _ = unsafe { libc::closedir(self.0.as_ptr()) };
    }
}
