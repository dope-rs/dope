use std::{ffi::CStr, mem, num::NonZero, os::fd};

use dope_sys::{Errno, kernel};

use super::{Event, Watch};

pub(super) fn create(path: &CStr) -> Result<fd::OwnedFd, Errno> {
    use fd::AsRawFd as _;

    // SAFETY: init transfers one descriptor or leaves this thread's errno.
    let file = unsafe {
        <fd::OwnedFd as dope_sys::io::fd::raw::Adopt>::try_adopt::<crate::Uring, Errno>(
            libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC),
        )
    }?;
    // SAFETY: the descriptor and terminated path stay live for this one registration.
    let raw = unsafe {
        libc::inotify_add_watch(
            file.as_raw_fd(),
            path.as_ptr(),
            libc::IN_MOVED_TO
                | libc::IN_MOVE_SELF
                | libc::IN_DELETE_SELF
                | libc::IN_ONLYDIR
                | libc::IN_DONT_FOLLOW,
        )
    };
    // SAFETY: add_watch returns a nonnegative watch id or -1 with unchanged errno.
    unsafe { kernel::raw::Status::new(raw).try_value::<crate::Uring, Errno>() }?;
    Ok(file)
}

pub struct Events<'a>(&'a [u8]);

/// # Safety
/// Bytes contain complete initialized inotify records with nonzero event masks.
pub unsafe fn events<'a>(_: &Watch, bytes: &'a [u8]) -> Events<'a> {
    Events(bytes)
}

impl<'a> Iterator for Events<'a> {
    type Item = Event<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        const HEADER: usize = mem::size_of::<libc::inotify_event>();
        // SAFETY: the retained native prefix consists of complete initialized records.
        let event = unsafe {
            self.0
                .as_ptr()
                .cast::<libc::inotify_event>()
                .read_unaligned()
        };
        // SAFETY: the native length bounds this complete record within the admitted read.
        let (record, rest) = unsafe { self.0.split_at_unchecked(HEADER + event.len as usize) };
        self.0 = rest;
        Some(match event.mask & !libc::IN_ISDIR {
            libc::IN_MOVED_TO => Event::MovedIn(&record[HEADER..]),
            libc::IN_Q_OVERFLOW => Event::Overflow,
            // SAFETY: every admitted native record carries a nonzero event mask.
            _ => Event::Invalidated(unsafe { NonZero::new_unchecked(event.mask) }),
        })
    }
}
