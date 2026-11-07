use std::{marker, mem, os::fd, ptr};

use dope_sys::io::{
    file::{Mode, Offset, Open, Sync},
    reactor::complete,
    request,
};
use o3::buffer::bytes;

use crate::reactor::{op::files, sq, types};

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

    fn file_write(
        file: fd::BorrowedFd<'a>,
        input: bytes::Prefix<'a, { i32::MAX as usize }>,
        offset: u64,
    ) -> Self {
        let input = input.as_slice();
        let raw = fd::AsRawFd::as_raw_fd(&file);
        let mut entry = sq::Entry::new(files::WRITE, sq::Descriptor::File(raw));
        let fields = entry.fields();
        fields.address = input.as_ptr() as u64;
        fields.length = input.len() as u32;
        fields.offset = offset.to_ne_bytes();
        Self::new(entry.flags(sq::Flags::ASYNC))
    }
}

impl<'a> request::Request<'a> for Borrowed<'a> {
    type Buffer = bytes::Prefix<'a, { i32::MAX as usize }>;
    type Options = types::OpenHow;
    type Metadata = crate::raw::Metadata;

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
        let fields = entry.fields();
        fields.address = path.as_ptr() as u64;
        fields.length = mem::size_of::<Self::Options>() as u32;
        fields.offset = (ptr::from_ref(options) as u64).to_ne_bytes();
        Self::new(entry)
    }

    fn metadata(file: fd::BorrowedFd<'a>, output: &'a mut Self::Metadata) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&file);
        let mut entry = sq::Entry::new(files::METADATA, sq::Descriptor::File(raw));
        let fields = entry.fields();
        fields.address = c"".as_ptr() as u64;
        fields.length = Self::Metadata::MASK | libc::STATX_INO;
        fields.offset = (output.0.as_mut_ptr() as u64).to_ne_bytes();
        fields.options = libc::AT_EMPTY_PATH as u32;
        Self::new(entry.flags(sq::Flags::ASYNC))
    }

    fn write(file: fd::BorrowedFd<'a>, input: Self::Buffer, offset: Offset) -> Self {
        Self::file_write(file, input, offset.get())
    }

    fn read(
        file: fd::BorrowedFd<'a>,
        output: bytes::bounded::Value<&'a mut [mem::MaybeUninit<u8>]>,
        offset: Offset,
    ) -> Self {
        let output = output.into_inner();
        let raw = fd::AsRawFd::as_raw_fd(&file);
        let mut entry = sq::Entry::new(files::READ, sq::Descriptor::File(raw));
        let fields = entry.fields();
        fields.address = output.as_mut_ptr() as u64;
        fields.length = output.len() as u32;
        fields.offset = offset.get().to_ne_bytes();
        Self::new(entry.flags(sq::Flags::ASYNC))
    }

    fn write_current(file: fd::BorrowedFd<'a>, input: Self::Buffer) -> Self {
        // io_uring's -1 offset selects the descriptor's current position.
        Self::file_write(file, input, u64::MAX)
    }

    fn sync(file: fd::BorrowedFd<'a>, mode: Sync) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&file);
        let mut entry = sq::Entry::new(files::SYNC, sq::Descriptor::File(raw));
        entry.fields().options = mode as u32;
        Self::new(entry.flags(sq::Flags::ASYNC))
    }

    fn rename(dir: fd::BorrowedFd<'a>, old: &'a std::ffi::CStr, new: &'a std::ffi::CStr) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&dir);
        let mut entry = sq::Entry::new(files::RENAME, sq::Descriptor::File(raw));
        let fields = entry.fields();
        fields.address = old.as_ptr() as u64;
        fields.length = raw as u32;
        fields.offset = (new.as_ptr() as u64).to_ne_bytes();
        Self::new(entry.flags(sq::Flags::ASYNC))
    }

    fn unlink(dir: fd::BorrowedFd<'a>, name: &'a std::ffi::CStr) -> Self {
        let raw = fd::AsRawFd::as_raw_fd(&dir);
        let mut entry = sq::Entry::new(files::UNLINK, sq::Descriptor::File(raw));
        entry.fields().address = name.as_ptr() as u64;
        Self::new(entry.flags(sq::Flags::ASYNC))
    }
}

const _: () = {
    assert!(mem::size_of::<Borrowed<'static>>() == mem::size_of::<sq::Entry>());
    assert!(mem::size_of::<Option<Borrowed<'static>>>() == 72);
};
