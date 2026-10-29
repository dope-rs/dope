use std::os::fd;

use dope_sys::io::reactor::complete;

use crate::reactor::{op, ring, sq};

pub(crate) const CANCEL: op::Code = op::Code::new(ring::ffi::ASYNC_CANCEL);

pub(crate) const POLL: op::Code = op::Code::new(ring::ffi::POLL_ADD);

pub(crate) fn poll(file: fd::BorrowedFd<'_>, multi: bool) -> sq::Borrowed<'_> {
    const ROTATE: u32 = if u16::from_ne_bytes([1, 0]) == 1 {
        0
    } else {
        16
    };

    let raw = fd::AsRawFd::as_raw_fd(&file);
    let mut entry = sq::Entry::new(POLL, sq::Descriptor::File(raw));
    let mut fields = entry.fields();
    if multi {
        fields.length(1);
    }
    let readable = u32::from(libc::POLLIN.unsigned_abs());
    fields.options(readable.rotate_left(ROTATE));
    sq::Borrowed::new(entry)
}

pub(crate) fn cancel<T: complete::raw::Token>(token: T, all: bool) -> sq::Entry {
    let mut entry = sq::Entry::new(CANCEL, sq::Descriptor::File(-1));
    entry.fields().address(sq::raw::Word::encode(token).get());
    if all {
        entry.fields().options(1);
    }
    entry
}
