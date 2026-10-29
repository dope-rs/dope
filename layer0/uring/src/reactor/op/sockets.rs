use std::{mem, ptr};

use dope_sys::io::socket::option::{Kind, Source as _};

use crate::{
    Uring,
    reactor::{op, ring, sq},
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Direction {
    Write,
    Both,
}

pub(crate) const CREATE: op::Code = op::Code::new(ring::ffi::SOCKET);

pub(crate) const CONNECT: op::Code = op::Code::new(ring::ffi::CONNECT);

pub(crate) const ACCEPT: op::Code = op::Code::new(ring::ffi::ACCEPT);

pub(crate) const SHUTDOWN: op::Code = op::Code::new(ring::ffi::SHUTDOWN);

pub(crate) const SET_OPTION: op::Code = op::Code::new(ring::ffi::URING_CMD);

/// Direct descriptors use the ring's nonblocking issue and asynchronous retry path.
pub(crate) fn accept(file: sq::Fixed) -> sq::Entry {
    use crate::reactor::ring::ffi;

    let mut entry = sq::Entry::new(ACCEPT, sq::Descriptor::Fixed(file));
    entry.fields().index(ffi::FILE_INDEX_ALLOC);
    entry
}

pub(crate) fn shutdown(file: sq::Fixed, how: Direction) -> sq::Entry {
    let mut entry = sq::Entry::new(SHUTDOWN, sq::Descriptor::Fixed(file));
    entry.fields().length(how.raw() as u32);
    entry
}

impl Direction {
    const fn raw(self) -> i32 {
        match self {
            Self::Write => libc::SHUT_WR,
            Self::Both => libc::SHUT_RDWR,
        }
    }
}

pub(crate) fn set_option(file: sq::Fixed, option: Kind, value: &libc::c_int) -> sq::Borrowed<'_> {
    use crate::reactor::ring::ffi;

    let (level, name) = Uring::option(option);
    let mut pair = [0; 8];
    pair[..4].copy_from_slice(&level.raw().to_ne_bytes());
    pair[4..].copy_from_slice(&name.raw().to_ne_bytes());
    let mut entry = sq::Entry::new(SET_OPTION, sq::Descriptor::Fixed(file));
    let mut fields = entry.fields();
    fields.command(ffi::SOCKET_SETSOCKOPT);
    fields.address(u64::from_ne_bytes(pair));
    fields.index(mem::size_of::<libc::c_int>() as u32);
    fields.address3(ptr::from_ref(value) as u64);
    sq::Borrowed::new(entry)
}
