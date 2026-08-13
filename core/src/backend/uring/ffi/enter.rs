use core::time;
use std::{
    io, mem,
    os::fd::{self, AsRawFd as _},
    ptr,
};

use io_uring::squeue;

use crate::platform;

const REGISTER_RING_FDS: libc::c_long = 20;
const UNREGISTER_RING_FDS: libc::c_long = 21;

const ENTER_GETEVENTS: u32 = 1 << 0;
const ENTER_SQ_WAKEUP: u32 = 1 << 1;
const ENTER_EXT_ARG: u32 = 1 << 3;
const ENTER_REGISTERED_RING: u32 = 1 << 4;

#[repr(C)]
struct ResourceUpdate {
    offset: u32,
    reserved: u32,
    data: u64,
}

#[repr(C)]
struct KernelTimespec {
    seconds: i64,
    nanoseconds: i64,
}

#[repr(C)]
struct GetEventsArg {
    signal_mask: u64,
    signal_mask_size: u32,
    minimum_wait_microseconds: u32,
    timespec: u64,
}

/// Task-local registered-ring authority retained across kernel-visible memory.
pub(in crate::backend::uring) struct RegisteredEnter {
    ring: fd::RawFd,
    index: u32,
    mode: Mode,
    _thread: o3::ThreadBound,
}

#[derive(Clone, Copy)]
struct Mode {
    iopoll: bool,
    sqpoll: bool,
    nodrop: bool,
}

enum Prepared {
    Skip(usize),
    Enter {
        submissions: u32,
        completions: u32,
        flags: u32,
    },
}

impl RegisteredEnter {
    pub(in crate::backend::uring) fn register(ring: &io_uring::IoUring) -> io::Result<Self> {
        let raw = ring.as_raw_fd();
        let mut update = ResourceUpdate {
            offset: u32::MAX,
            reserved: 0,
            data: raw as u64,
        };
        register_call(raw, REGISTER_RING_FDS, &mut update)?;
        Ok(Self {
            ring: raw,
            index: update.offset,
            mode: Mode {
                iopoll: ring.params().is_setup_iopoll(),
                sqpoll: ring.params().is_setup_sqpoll(),
                nodrop: ring.params().is_feature_nodrop(),
            },
            _thread: o3::ThreadBound::NEW,
        })
    }

    pub(in crate::backend::uring) fn unregister(&self) -> io::Result<()> {
        let mut update = ResourceUpdate {
            offset: self.index,
            reserved: 0,
            data: 0,
        };
        register_call(self.ring, UNREGISTER_RING_FDS, &mut update)
    }

    pub(in crate::backend::uring) fn submit(
        &self,
        submission: &squeue::SubmissionQueue<'_>,
    ) -> io::Result<usize> {
        self.enter_prepared(
            submission,
            0,
            0,
            ptr::null(),
            mem::size_of::<libc::sigset_t>(),
        )
    }

    pub(in crate::backend::uring) fn wait(
        &self,
        submission: &squeue::SubmissionQueue<'_>,
        timeout: Option<time::Duration>,
    ) -> io::Result<()> {
        let result = match timeout {
            Some(timeout) => {
                let timeout = platform::Timeout::try_from(timeout)?;
                let timespec = KernelTimespec {
                    seconds: timeout.seconds(),
                    nanoseconds: timeout.nanoseconds(),
                };
                let arguments = GetEventsArg {
                    signal_mask: 0,
                    signal_mask_size: 0,
                    minimum_wait_microseconds: 0,
                    timespec: ptr::from_ref(&timespec) as u64,
                };
                self.enter_prepared(
                    submission,
                    1,
                    ENTER_EXT_ARG,
                    ptr::from_ref(&arguments).cast(),
                    mem::size_of::<GetEventsArg>(),
                )
            }
            None => self.enter_prepared(
                submission,
                1,
                0,
                ptr::null(),
                mem::size_of::<libc::sigset_t>(),
            ),
        };
        match result {
            Ok(_) => Ok(()),
            Err(error) if matches!(error.raw_os_error(), Some(libc::ETIME | libc::EINTR)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn enter_prepared(
        &self,
        submission: &squeue::SubmissionQueue<'_>,
        completions: usize,
        initial_flags: u32,
        arguments: *const libc::c_void,
        argument_size: usize,
    ) -> io::Result<usize> {
        match prepare(submission, completions, initial_flags, self.mode)? {
            Prepared::Skip(submissions) => Ok(submissions),
            Prepared::Enter {
                submissions,
                completions,
                flags,
            } => self.enter(submissions, completions, flags, arguments, argument_size),
        }
    }

    fn enter(
        &self,
        submissions: u32,
        completions: u32,
        flags: u32,
        arguments: *const libc::c_void,
        argument_size: usize,
    ) -> io::Result<usize> {
        // SAFETY: index is registered on this thread, the scalar arguments
        // match io_uring_enter, and arguments names argument_size live bytes.
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                self.index as libc::c_long,
                submissions as libc::c_long,
                completions as libc::c_long,
                (flags | ENTER_REGISTERED_RING) as libc::c_long,
                arguments as usize as libc::c_long,
                argument_size as libc::c_long,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }
}

fn prepare(
    submission: &squeue::SubmissionQueue<'_>,
    completions: usize,
    initial_flags: u32,
    mode: Mode,
) -> io::Result<Prepared> {
    let submissions = u32::try_from(submission.len())
        .map_err(|_| io::Error::other("dope: io_uring submission count exceeds u32"))?;
    let completions = u32::try_from(completions)
        .map_err(|_| io::Error::other("dope: io_uring completion count exceeds u32"))?;
    let overflow = submission.cq_overflow();
    let mut flags = initial_flags;
    if completions != 0 || mode.iopoll || overflow {
        flags |= ENTER_GETEVENTS;
    }
    let wakeup = mode.sqpoll && submission.need_wakeup();
    if mode.sqpoll {
        if wakeup {
            flags |= ENTER_SQ_WAKEUP;
        } else if completions == 0 && !(overflow && mode.nodrop) {
            return Ok(Prepared::Skip(submissions as usize));
        }
    }
    Ok(Prepared::Enter {
        submissions,
        completions,
        flags,
    })
}

fn register_call(
    ring: fd::RawFd,
    operation: libc::c_long,
    update: &mut ResourceUpdate,
) -> io::Result<()> {
    loop {
        // SAFETY: ring is a live io_uring fd and ResourceUpdate is the exact
        // registration ABI payload for one ring-table entry.
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                ring as libc::c_long,
                operation,
                ptr::from_mut(update) as usize as libc::c_long,
                1 as libc::c_long,
            )
        };
        if result >= 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

const _: () = {
    assert!(mem::size_of::<ResourceUpdate>() == 16);
    assert!(mem::size_of::<KernelTimespec>() == 16);
    assert!(mem::size_of::<GetEventsArg>() == 24);
    assert!(mem::size_of::<Mode>() == 3);
};
