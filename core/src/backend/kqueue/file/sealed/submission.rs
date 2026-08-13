use std::{
    io, mem,
    os::{fd, fd::FromRawFd as _},
};

use crate::{
    backend::{
        self, bound,
        kqueue::{engine::event, errno},
    },
    driver::{self, flight, route},
    io::{event::open, fs, transfer},
};

const COMPONENT_MAX: usize = 255;

unsafe extern "C" {
    #[link_name = "fdatasync"]
    fn sync_data(fd: libc::c_int) -> libc::c_int;
}

enum Operation {
    Open {
        dir: fd::RawFd,
        path: *const libc::c_char,
        flags: i32,
    },
    Read {
        fd: fd::RawFd,
        ptr: *mut u8,
        len: transfer::Len,
        offset: Option<libc::off_t>,
    },
    Write {
        fd: fd::RawFd,
        ptr: *const u8,
        len: transfer::Len,
        offset: Option<libc::off_t>,
    },
    Stat {
        fd: fd::RawFd,
        output: *mut libc::stat,
    },
    Sync {
        fd: fd::RawFd,
        mode: fs::Sync,
    },
}

pub(crate) struct Submission(Operation);

pub(super) enum Completion {
    Open {
        key: flight::raw::Echo,
        result: Result<fd::OwnedFd, open::Error>,
    },
    Read {
        key: flight::raw::Echo,
        result: i32,
    },
    Write {
        key: flight::raw::Echo,
        result: i32,
    },
    Stat {
        key: flight::raw::Echo,
        result: i32,
    },
    Sync {
        key: flight::raw::Echo,
        result: i32,
    },
}

// SAFETY: `key` is an opaque echo of a live retained-flight reservation and
// is never dereferenced by the worker. The reactor receives it back unchanged
// before the retained owner can be released.
unsafe impl Send for Completion {}

impl Submission {
    pub(super) fn execute(self, key: flight::raw::Echo) -> Completion {
        match self.0 {
            Operation::Open { dir, path, flags } => Completion::Open {
                key,
                result: Self::open_beneath(dir, path, flags),
            },
            Operation::Read {
                fd,
                ptr,
                len,
                offset,
            } => Completion::Read {
                key,
                result: match offset {
                    Some(offset) => Self::io_result(unsafe {
                        libc::pread(fd, ptr.cast(), len.into_usize(), offset)
                    }),
                    None => -libc::EOVERFLOW,
                },
            },
            Operation::Write {
                fd,
                ptr,
                len,
                offset,
            } => Completion::Write {
                key,
                result: match offset {
                    Some(offset) => Self::io_result(unsafe {
                        libc::pwrite(fd, ptr.cast(), len.into_usize(), offset)
                    }),
                    None => -libc::EOVERFLOW,
                },
            },
            Operation::Stat { fd, output } => Completion::Stat {
                key,
                result: Self::io_result(unsafe { libc::fstat(fd, output) } as isize),
            },
            Operation::Sync { fd, mode } => Completion::Sync {
                key,
                result: Self::io_result(unsafe {
                    match mode {
                        fs::Sync::Data => sync_data(fd),
                        fs::Sync::All => libc::fsync(fd),
                    }
                } as isize),
            },
        }
    }

    fn io_result(result: isize) -> i32 {
        if result < 0 {
            -errno::Errno::last().raw()
        } else {
            debug_assert!(result <= i32::MAX as isize);
            result as i32
        }
    }

    fn open_result(raw: fd::RawFd) -> Result<fd::OwnedFd, open::Error> {
        if raw < 0 {
            return Err(open::Error::from_errno(errno::Errno::last().raw()));
        }
        Ok(unsafe { fd::OwnedFd::from_raw_fd(raw) })
    }

    fn open_beneath(
        dir: fd::RawFd,
        path: *const libc::c_char,
        flags: i32,
    ) -> Result<fd::OwnedFd, open::Error> {
        let path = unsafe { std::ffi::CStr::from_ptr(path) };
        let mut components = path
            .to_bytes()
            .split(|&byte| byte == b'/')
            .filter(|component| !component.is_empty() && *component != b".")
            .peekable();
        let mut parent: Option<fd::OwnedFd> = None;
        let mut component_path = [0_u8; COMPONENT_MAX + 1];
        while let Some(component) = components.next() {
            if component.len() > COMPONENT_MAX {
                return Err(open::Error::from_errno(libc::ENAMETOOLONG));
            }
            component_path[..component.len()].copy_from_slice(component);
            component_path[component.len()] = 0;
            let final_component = components.peek().is_none();
            let component_flags = if final_component {
                flags | libc::O_NOFOLLOW
            } else {
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW
            };
            let current = parent.as_ref().map_or(dir, fd::AsRawFd::as_raw_fd);
            let opened = unsafe {
                libc::openat(current, component_path.as_ptr().cast(), component_flags, 0)
            };
            let opened = Self::open_result(opened)?;
            if final_component {
                return Ok(opened);
            }
            parent = Some(opened);
        }
        Err(open::Error::from_errno(libc::EINVAL))
    }
}

impl Completion {
    pub(super) fn into_event(self) -> event::Completion {
        match self {
            Self::Open { key, result } => match result {
                Ok(fd) => event::Completion::Opened { ud: key, fd },
                Err(error) => event::Completion::OpenFailed { ud: key, error },
            },
            Self::Read { key, result } => event::Completion::Read { ud: key, result },
            Self::Write { key, result } => event::Completion::Write { ud: key, result },
            Self::Stat { key, result } => event::Completion::Stat { ud: key, result },
            Self::Sync { key, result } => event::Completion::Sync { ud: key, result },
        }
    }
}

// SAFETY: each constructor captures its exact borrowed operands, and submit
// accepts them only after a retained owner and exact flight reservation have
// been bound. Quiescence joins the worker before releasing that owner.
unsafe impl fs::raw::Mode for fs::Native {
    type Raw = Submission;
    type Beneath = i32;
    type Metadata = libc::stat;

    fn confined_regular_open() -> Self::Beneath {
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NONBLOCK
    }

    fn parse(raw: &Self::Metadata) -> io::Result<fs::RawMetadata> {
        Ok(fs::RawMetadata {
            len: raw.st_size as u64,
            modified: Some((raw.st_mtime, raw.st_mtime_nsec as u32)),
            regular: raw.st_mode & libc::S_IFMT == libc::S_IFREG,
        })
    }

    fn open(request: &Self::Beneath, dir: fd::BorrowedFd<'_>, path: &std::ffi::CStr) -> Self::Raw {
        Submission(Operation::Open {
            dir: fd::AsRawFd::as_raw_fd(&dir),
            path: path.as_ptr(),
            flags: *request,
        })
    }

    fn read(
        fd: fd::BorrowedFd<'_>,
        buffer: &mut [mem::MaybeUninit<u8>],
        len: transfer::Len,
        offset: u64,
    ) -> Self::Raw {
        use libc::off_t;

        Submission(Operation::Read {
            fd: fd::AsRawFd::as_raw_fd(&fd),
            ptr: buffer.as_mut_ptr().cast(),
            len,
            offset: off_t::try_from(offset).ok(),
        })
    }

    fn write(fd: fd::BorrowedFd<'_>, buffer: &[u8], len: transfer::Len, offset: u64) -> Self::Raw {
        use libc::off_t;

        Submission(Operation::Write {
            fd: fd::AsRawFd::as_raw_fd(&fd),
            ptr: buffer.as_ptr(),
            len,
            offset: off_t::try_from(offset).ok(),
        })
    }

    fn sync(fd: fd::BorrowedFd<'_>, mode: fs::Sync) -> Self::Raw {
        Submission(Operation::Sync {
            fd: fd::AsRawFd::as_raw_fd(&fd),
            mode,
        })
    }

    fn stat(
        fd: fd::BorrowedFd<'_>,
        output: &mut mem::MaybeUninit<fs::Metadata<Self>>,
    ) -> Self::Raw {
        Submission(Operation::Stat {
            fd: fd::AsRawFd::as_raw_fd(&fd),
            output: output.as_mut_ptr().cast(),
        })
    }

    fn submit<'owner, 'd: 'owner>(
        backend: &mut backend::Backend,
        submission: bound::Bound<'owner, 'd, Self::Raw>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        if backend.poll.is_failed() || backend.pending.is_full() {
            return Err(driver::SubmitError);
        }
        backend.file.submit(submission)
    }
}

const _: () = {
    assert!(mem::size_of::<fs::Native>() == 0);
    assert!(
        mem::size_of::<fs::Submission<'static, 'static, fs::Native, route::KeyTag<1>>>()
            == mem::size_of::<(Submission, route::Operation<'static, route::KeyTag<1>>,)>()
    );
    assert!(mem::size_of::<fs::Metadata<fs::Native>>() == mem::size_of::<libc::stat>());
};
