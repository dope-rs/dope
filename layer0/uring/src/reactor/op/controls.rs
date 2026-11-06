use std::os::fd;

use dope_sys::{engine::register, io::reactor::complete};

use crate::reactor::{op, ring, sq};

pub(crate) const CANCEL: op::Code = op::Code::new(ring::ffi::ASYNC_CANCEL);

pub(crate) const POLL: op::Code = op::Code::new(ring::ffi::POLL_ADD);

pub(crate) fn poll(source: register::Source<'_>, multi: bool) -> sq::Borrowed<'_> {
    const ROTATE: u32 = if u16::from_ne_bytes([1, 0]) == 1 {
        0
    } else {
        16
    };

    let raw = fd::AsRawFd::as_raw_fd(&source.fd);
    let mut entry = sq::Entry::new(POLL, sq::Descriptor::File(raw));
    let mut fields = entry.fields();
    if multi {
        fields.length(1);
    }
    let interest = match source.interest {
        register::Interest::Readable => libc::POLLIN,
        register::Interest::Closed => libc::POLLRDHUP,
    };
    fields.options(u32::from(interest.unsigned_abs()).rotate_left(ROTATE));
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
