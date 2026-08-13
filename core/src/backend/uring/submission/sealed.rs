use std::{io, mem, os::fd};

use io_uring::{opcode, squeue, types};

use crate::{
    backend::{
        self, bound,
        uring::{self, submission},
    },
    driver::{self, flight},
    io::{fs, transfer},
    platform,
};

// SAFETY: each SQE captures the exact operation pointers and io_uring retains
// them until a terminal CQE or ring quiescence. Metadata is transparent over
// statx, and submit receives raw work only after the retained owner is bound.
unsafe impl fs::raw::Mode for fs::Native {
    type Raw = submission::RawSubmission;
    type Beneath = types::OpenHow;
    type Metadata = libc::statx;

    fn confined_regular_open() -> Self::Beneath {
        types::OpenHow::new()
            .flags(u64::from(
                (libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NONBLOCK) as u32,
            ))
            .resolve(libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS)
    }

    fn parse(raw: &Self::Metadata) -> io::Result<fs::RawMetadata> {
        use libc::{S_IFMT, S_IFREG};

        Ok(fs::RawMetadata {
            len: raw.stx_size,
            modified: Some((raw.stx_mtime.tv_sec, raw.stx_mtime.tv_nsec)),
            regular: u32::from(raw.stx_mode) & S_IFMT == S_IFREG,
        })
    }

    fn open(request: &Self::Beneath, dir: fd::BorrowedFd<'_>, path: &std::ffi::CStr) -> Self::Raw {
        submission::RawSubmission::new(
            opcode::OpenAt2::new(
                types::Fd(fd::AsRawFd::as_raw_fd(&dir)),
                path.as_ptr(),
                request,
            )
            .build(),
        )
    }

    fn read(
        fd: fd::BorrowedFd<'_>,
        buffer: &mut [mem::MaybeUninit<u8>],
        len: transfer::Len,
        offset: u64,
    ) -> Self::Raw {
        submission::RawSubmission::new(
            opcode::Read::new(
                types::Fd(fd::AsRawFd::as_raw_fd(&fd)),
                buffer.as_mut_ptr().cast(),
                len.get(),
            )
            .offset(offset)
            .build(),
        )
    }

    fn write(fd: fd::BorrowedFd<'_>, buffer: &[u8], len: transfer::Len, offset: u64) -> Self::Raw {
        submission::RawSubmission::new(
            opcode::Write::new(
                types::Fd(fd::AsRawFd::as_raw_fd(&fd)),
                buffer.as_ptr(),
                len.get(),
            )
            .offset(offset)
            .build()
            .flags(squeue::Flags::ASYNC),
        )
    }

    fn sync(fd: fd::BorrowedFd<'_>, mode: fs::Sync) -> Self::Raw {
        let flags = match mode {
            fs::Sync::Data => types::FsyncFlags::DATASYNC,
            fs::Sync::All => types::FsyncFlags::empty(),
        };
        submission::RawSubmission::new(
            opcode::Fsync::new(types::Fd(fd::AsRawFd::as_raw_fd(&fd)))
                .flags(flags)
                .build()
                .flags(squeue::Flags::ASYNC),
        )
    }

    fn stat(
        fd: fd::BorrowedFd<'_>,
        output: &mut mem::MaybeUninit<fs::Metadata<Self>>,
    ) -> Self::Raw {
        submission::RawSubmission::new(
            opcode::Statx::new(
                types::Fd(fd::AsRawFd::as_raw_fd(&fd)),
                c"".as_ptr(),
                output.as_mut_ptr().cast::<types::statx>(),
            )
            .flags(libc::AT_EMPTY_PATH)
            .mask(libc::STATX_TYPE | libc::STATX_SIZE | libc::STATX_MTIME)
            .build(),
        )
    }

    fn submit<'owner, 'd: 'owner>(
        backend: &mut backend::Backend,
        submission: bound::Bound<'owner, 'd, Self::Raw>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        use platform::reactor;
        use uring::engine::submit;

        let mut queue = submit::Queue::new(backend);
        reactor::Queue::submit(&mut queue, submission)
    }
}

const _: () = {
    assert!(mem::size_of::<fs::Native>() == 0);
    assert!(
        mem::size_of::<fs::Submission<'static, 'static, fs::Native, driver::route::KeyTag<1>>>()
            == mem::size_of::<(
                submission::RawSubmission,
                driver::route::Operation<'static, driver::route::KeyTag<1>>,
            )>()
    );
    assert!(mem::size_of::<fs::Metadata<fs::Native>>() == mem::size_of::<libc::statx>());
};
