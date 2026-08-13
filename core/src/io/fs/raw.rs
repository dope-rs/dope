use std::{io, marker, mem, os::fd, slice};

use crate::{
    backend::{self, bound},
    driver::{self, flight, retained, route},
    io::{fs, transfer},
};

pub(crate) struct Retained<'owner, 'd: 'owner, Tag: route::Tag, R> {
    raw: backend::Captured<'owner, R>,
    target: route::Operation<'d, Tag>,
    owner: marker::PhantomData<fn(&'owner ()) -> &'owner ()>,
}

const _: () = {
    type Retained = self::Retained<'static, 'static, route::KeyTag<1>, usize>;
    type Parts = (usize, route::Operation<'static, route::KeyTag<1>>);
    assert!(mem::size_of::<Retained>() == mem::size_of::<Parts>());
    assert!(mem::align_of::<Retained>() == mem::align_of::<Parts>());
};

/// Proof that a file mode preserves native borrows through backend quiescence.
/// # Safety
/// Associated representations and every captured borrow must remain exact.
pub(crate) unsafe trait Mode: Sized {
    type Raw;
    type Beneath;
    type Metadata;

    fn confined_regular_open() -> Self::Beneath;
    fn parse(raw: &Self::Metadata) -> io::Result<fs::RawMetadata>;
    fn open<'a>(
        request: &'a Self::Beneath,
        dir: fd::BorrowedFd<'a>,
        path: &'a std::ffi::CStr,
    ) -> Self::Raw;
    fn read<'a>(
        fd: fd::BorrowedFd<'a>,
        buffer: &'a mut [mem::MaybeUninit<u8>],
        len: transfer::Len,
        offset: u64,
    ) -> Self::Raw;
    fn write<'a>(
        fd: fd::BorrowedFd<'a>,
        buffer: &'a [u8],
        len: transfer::Len,
        offset: u64,
    ) -> Self::Raw;
    fn sync(fd: fd::BorrowedFd<'_>, mode: fs::Sync) -> Self::Raw;
    fn read_initialized<'a>(
        fd: fd::BorrowedFd<'a>,
        buffer: &'a mut [u8],
        len: transfer::Len,
        offset: u64,
    ) -> Self::Raw {
        // SAFETY: all bytes start initialized. The backend may replace bytes,
        // while untouched bytes remain initialized when the borrow ends.
        let buffer = unsafe {
            slice::from_raw_parts_mut(
                buffer.as_mut_ptr().cast::<mem::MaybeUninit<u8>>(),
                buffer.len(),
            )
        };
        Self::read(fd, buffer, len, offset)
    }
    fn stat<'a>(
        fd: fd::BorrowedFd<'a>,
        output: &'a mut mem::MaybeUninit<fs::Metadata<Self>>,
    ) -> Self::Raw
    where
        Self: fs::Mode;
    fn submit<'owner, 'd: 'owner>(
        backend: &mut backend::Backend,
        submission: bound::Bound<'owner, 'd, Self::Raw>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>;
}

impl<'owner, 'd: 'owner, Tag, R> Retained<'owner, 'd, Tag, R>
where
    Tag: route::Tag,
{
    pub(crate) unsafe fn bind<'borrow, F>(
        context: &retained::Context<'_, 'owner, 'd>,
        submission: fs::Submission<'borrow, 'd, F, Tag>,
    ) -> Self
    where
        F: fs::Mode + Mode<Raw = R>,
        'owner: 'borrow,
    {
        Self {
            raw: unsafe { backend::raw::Retainer::new(context).bind(submission.raw) },
            target: submission.target,
            owner: marker::PhantomData,
        }
    }

    pub(crate) fn into_parts(self) -> (backend::Captured<'owner, R>, route::Operation<'d, Tag>) {
        (self.raw, self.target)
    }
}
