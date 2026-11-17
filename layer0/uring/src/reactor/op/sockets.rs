use std::{mem, ptr};

use dope_sys::io::socket::{
    How,
    option::{Kind, Source as _},
};

use crate::{
    Uring,
    reactor::{op, ring, sq},
};

pub(crate) const CREATE: op::Code = op::Code::new(ring::ffi::SOCKET);

pub(crate) const CONNECT: op::Code = op::Code::new(ring::ffi::CONNECT);

pub(crate) const ACCEPT: op::Code = op::Code::new(ring::ffi::ACCEPT);

pub(crate) const SHUTDOWN: op::Code = op::Code::new(ring::ffi::SHUTDOWN);

pub(crate) const OPTION: op::Code = op::Code::new(ring::ffi::URING_CMD);

/// Direct descriptors use the ring's nonblocking issue and asynchronous retry path.
pub(crate) fn accept(file: sq::Fixed) -> sq::Entry {
    use crate::reactor::ring::ffi;

    let mut entry = sq::Entry::new(ACCEPT, sq::Descriptor::Fixed(file));
    entry.fields().index = ffi::FILE_INDEX_ALLOC;
    entry
}

pub(crate) fn shutdown(file: sq::Fixed, how: How) -> sq::Entry {
    let mut entry = sq::Entry::new(SHUTDOWN, sq::Descriptor::Fixed(file));
    entry.fields().length = how.raw() as u32;
    entry
}

pub(crate) fn set_option(file: sq::Fixed, option: Kind, value: &libc::c_int) -> sq::Borrowed<'_> {
    use crate::reactor::ring::ffi;

    let (level, name) = Uring::option(option);
    let mut entry = command(file, ffi::SOCKET_SETSOCKOPT, level, name);
    let fields = entry.fields();
    fields.index = mem::size_of::<libc::c_int>() as u32;
    fields.address3 = ptr::from_ref(value) as u64;
    sq::Borrowed::new(entry)
}

pub(crate) fn peer_uid<const N: usize>(
    file: sq::Fixed,
    output: &mut [libc::c_int; N],
) -> sq::Borrowed<'_> {
    const {
        assert!(N >= 3);
        assert!(mem::size_of::<libc::ucred>() == 3 * mem::size_of::<libc::c_int>());
        assert!(mem::align_of::<libc::ucred>() == mem::align_of::<libc::c_int>());
        assert!(mem::offset_of!(libc::ucred, uid) == mem::size_of::<libc::c_int>());
        assert!(mem::size_of::<libc::uid_t>() == mem::size_of::<u32>());
    }
    let mut entry = command(
        file,
        ring::ffi::SOCKET_GETSOCKOPT,
        libc::SOL_SOCKET,
        libc::SO_PEERCRED,
    );
    entry.fields().index = mem::size_of::<libc::ucred>() as u32;
    entry.fields().address3 = output.as_mut_ptr() as u64;
    sq::Borrowed::new(entry)
}

fn command(file: sq::Fixed, command: u32, level: libc::c_int, name: libc::c_int) -> sq::Entry {
    let mut pair = [0; 8];
    pair[..4].copy_from_slice(&level.to_ne_bytes());
    pair[4..].copy_from_slice(&name.to_ne_bytes());
    let mut entry = sq::Entry::new(OPTION, sq::Descriptor::Fixed(file));
    let fields = entry.fields();
    fields.offset[..4].copy_from_slice(&command.to_ne_bytes());
    fields.address = u64::from_ne_bytes(pair);
    entry
}
