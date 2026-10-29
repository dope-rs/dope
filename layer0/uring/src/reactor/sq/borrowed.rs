use std::{marker, mem, os::fd, ptr};

use dope_sys::io::{
    self,
    file::{Mode, Offset, Open, Sync},
    reactor::complete,
    request,
    socket::addr,
};
use libc::socklen_t;
use o3::buffer::bytes;

use crate::reactor::{
    op::{buffers, files, messages, sockets},
    sq, types,
};

/// An SQE tied to every borrowed address encoded in it.
#[repr(transparent)]
pub struct Borrowed<'a>(pub(super) sq::Entry, marker::PhantomData<&'a mut ()>);

impl<'a> Borrowed<'a> {
    pub(crate) const fn new(entry: sq::Entry) -> Self {
        Self(entry, marker::PhantomData)
    }

    pub(crate) fn user_data<T: complete::raw::Token>(self, token: T) -> sq::Submit<'a, T> {
        self.0.bind(token)
    }
}

impl<'a> request::Borrow<'a> for Borrowed<'a> {
    type Target = sq::Fixed;
    type Buffer = bytes::Prefix<'a, { i32::MAX as usize }>;

    fn send(slot: Self::Target, input: Self::Buffer) -> Self {
        let input = input.into_slice();
        let mut entry = sq::Entry::new(buffers::SEND, sq::Descriptor::Fixed(slot));
        let mut fields = entry.fields();
        fields.address(input.as_ptr() as u64);
        fields.length(input.len() as u32);
        fields.options(libc::MSG_NOSIGNAL.unsigned_abs());
        Self::new(entry)
    }

    fn send_msg<M: io::raw::Message<'a>>(slot: Self::Target, message: M) -> Self {
        let header = message.header();
        let mut entry = sq::Entry::new(messages::SEND, sq::Descriptor::Fixed(slot));
        let mut fields = entry.fields();
        fields.address(ptr::from_ref(header) as u64);
        fields.length(1);
        fields.options(libc::MSG_NOSIGNAL.unsigned_abs());
        Self::new(entry)
    }

    fn accept(
        listener: Self::Target,
        address: &'a mut libc::sockaddr_storage,
        len: &'a mut socklen_t,
    ) -> Self {
        const CAPACITY: socklen_t = {
            assert!(mem::size_of::<libc::sockaddr_storage>() <= socklen_t::MAX as usize);
            mem::size_of::<libc::sockaddr_storage>() as socklen_t
        };

        *len = CAPACITY;
        let mut entry = sockets::accept(listener);
        let mut fields = entry.fields();
        fields.address(ptr::from_mut(address).cast::<libc::sockaddr>() as u64);
        fields.offset(ptr::from_mut(len) as u64);
        Self::new(entry)
    }

    fn connect(slot: Self::Target, address: addr::Addr<'a>) -> Self {
        let (address, len) = address.parts();
        let mut entry = sq::Entry::new(sockets::CONNECT, sq::Descriptor::Fixed(slot));
        let mut fields = entry.fields();
        fields.address(address as u64);
        fields.offset(len.get() as u64);
        Self::new(entry)
    }
}

impl<'a> request::Request<'a> for Borrowed<'a> {
    type Buffer = bytes::Prefix<'a, { i32::MAX as usize }>;
    type Options = types::OpenHow;

    fn options(flags: Open, mode: Mode) -> Self::Options {
        types::OpenHow::new(
            flags.raw() as u32 as u64,
            mode.raw() as u64,
            libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS,
        )
    }

    fn open(dir: fd::BorrowedFd<'a>, path: &'a std::ffi::CStr, options: &'a Self::Options) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&dir);
        let mut entry = sq::Entry::new(files::OPEN, sq::Descriptor::File(raw));
        let mut fields = entry.fields();
        fields.address(path.as_ptr() as u64);
        fields.length(mem::size_of::<Self::Options>() as u32);
        fields.offset(ptr::from_ref(options) as u64);
        Self::new(entry)
    }

    fn write(file: fd::BorrowedFd<'a>, input: Self::Buffer, offset: Offset) -> Self {
        let input = input.into_slice();
        let raw = fd::AsRawFd::as_raw_fd(&file);
        let mut entry = sq::Entry::new(files::WRITE, sq::Descriptor::File(raw));
        let mut fields = entry.fields();
        fields.address(input.as_ptr() as u64);
        fields.length(input.len() as u32);
        fields.offset(offset.get());
        Self::new(entry.flags(sq::Flags::ASYNC))
    }

    fn sync(file: fd::BorrowedFd<'a>, mode: Sync) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&file);
        let mut entry = sq::Entry::new(files::SYNC, sq::Descriptor::File(raw));
        entry.fields().options(mode as u32);
        Self::new(entry.flags(sq::Flags::ASYNC))
    }

    fn rename(dir: fd::BorrowedFd<'a>, old: &'a std::ffi::CStr, new: &'a std::ffi::CStr) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&dir);
        let mut entry = sq::Entry::new(files::RENAME, sq::Descriptor::File(raw));
        let mut fields = entry.fields();
        fields.address(old.as_ptr() as u64);
        fields.length(raw as u32);
        fields.offset(new.as_ptr() as u64);
        Self::new(entry.flags(sq::Flags::ASYNC))
    }

    fn unlink(dir: fd::BorrowedFd<'a>, name: &'a std::ffi::CStr) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&dir);
        let mut entry = sq::Entry::new(files::UNLINK, sq::Descriptor::File(raw));
        entry.fields().address(name.as_ptr() as u64);
        Self::new(entry.flags(sq::Flags::ASYNC))
    }
}

const _: () = assert!(mem::size_of::<Borrowed<'static>>() == mem::size_of::<sq::Entry>());
