use std::{
    mem::MaybeUninit,
    os::fd::{AsRawFd as _, OwnedFd},
};

use dope_sys::{Errno, io::file, kernel};
use o3::{alloc::BoxArrayExt as _, base::marker::ThreadBound};

use crate::Uring;

const CAPACITY: usize = 32 * 1024;

pub struct Stream {
    file: OwnedFd,
    buffer: Box<[MaybeUninit<u8>; CAPACITY]>,
    offset: usize,
    filled: usize,
    _thread: ThreadBound,
}

// SAFETY: the stream exclusively owns its descriptor and buffer. Refilling only
// writes the buffer after the previous Entry borrow has ended.
unsafe impl file::raw::Directory for Uring {
    type Stream = Stream;

    fn from_fd<E: From<Errno>>(file: OwnedFd) -> Result<Stream, file::OpenError<E>> {
        Ok(Stream {
            file,
            buffer: Box::try_uninit().map_err(file::OpenError::Allocation)?,
            offset: 0,
            filled: 0,
            _thread: ThreadBound::NEW,
        })
    }

    fn descriptor(stream: &Stream) -> kernel::raw::Descriptor {
        // SAFETY: the stream exclusively owns this live nonnegative descriptor.
        unsafe { kernel::raw::Descriptor::new(stream.file.as_raw_fd()) }
    }

    fn next<E: From<Errno>>(
        stream: &mut Stream,
    ) -> Result<Option<file::Entry<'_>>, file::ReadError<E>> {
        loop {
            if stream.offset == stream.filled {
                // SAFETY: the exclusively borrowed buffer supplies exactly CAPACITY
                // writable bytes; getdents64 reports complete native directory records.
                let result = unsafe {
                    libc::syscall(
                        libc::SYS_getdents64,
                        stream.file.as_raw_fd(),
                        stream.buffer.as_mut_ptr(),
                        CAPACITY,
                    )
                };
                // SAFETY: the exact syscall operand bounds every successful count by
                // CAPACITY; failure leaves this thread's positive errno unchanged.
                let count = unsafe {
                    kernel::raw::Status::from_count(result as libc::ssize_t)
                        .try_count::<Errno>(|| kernel::raw::Code::last::<Self>().into())
                };
                stream.filled = match count {
                    Ok(count) => count.get(),
                    Err(Errno::NOT_FOUND) => 0,
                    Err(error) => return Err(file::ReadError::Syscall(E::from(error))),
                };
                stream.offset = 0;
                if stream.filled == 0 {
                    return Ok(None);
                }
            }
            // SAFETY: the completed batch retains this record's initialized native
            // fields and terminated name. Each positive d_reclen advances to the next
            // complete record or filled; the cursor cannot leave the completed batch.
            // Padding remains MaybeUninit and is never read or exposed as bytes.
            let pointer = unsafe {
                stream
                    .buffer
                    .as_ptr()
                    .add(stream.offset)
                    .cast::<libc::dirent64>()
            };
            stream.offset += unsafe { (&raw const (*pointer).d_reclen).read_unaligned() } as usize;
            if unsafe { (&raw const (*pointer).d_ino).read_unaligned() } == 0 {
                continue;
            }
            let kind = unsafe { (&raw const (*pointer).d_type).read() };
            let name = unsafe { std::ffi::CStr::from_ptr((&raw const (*pointer).d_name).cast()) };
            return Ok(Some(file::Entry {
                name,
                kind: file::Kind::from_dirent(kind),
            }));
        }
    }
}
