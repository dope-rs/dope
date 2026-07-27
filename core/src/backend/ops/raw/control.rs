use std::io;
use std::os::fd::BorrowedFd;

use crate::backend::Backend;
use crate::driver::token::Token;
use crate::driver::{OutboundReservation, PushError};
use crate::io::fd::FdSlot;
use libc::c_int;

pub(crate) trait ControlBackend {
    fn prepare_drop(backend: &mut Backend) {
        backend.shutdown();
    }
    fn register_shutdown_fd(backend: &mut Backend, fd: BorrowedFd<'_>) -> io::Result<()>;
    fn reserve_outbound(backend: &mut Backend, count: u32) -> io::Result<OutboundReservation> {
        let base = backend.alloc_fixed_range(count)?;
        Ok(OutboundReservation::new(base, count))
    }
    fn reserve_route(backend: &mut Backend, id: u8) -> bool {
        backend.routes.reserve(id)
    }
    fn release_route(backend: &mut Backend, id: u8) {
        backend.routes.release(id);
    }
    fn poison_route(backend: &mut Backend, id: u8) {
        backend.routes.poison(id);
    }
    fn quiesce(backend: &mut Backend, targets: &[Token]) -> bool;
    fn submit_option(
        backend: &mut Backend,
        slot: FdSlot,
        level: c_int,
        name: c_int,
        value: c_int,
    ) -> Result<(), PushError>;
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io::{Error, ErrorKind};
    use std::os::fd::AsRawFd;
    use std::process::abort;

    use io_uring::opcode::SetSockOpt;
    use io_uring::types::{CancelBuilder, Fixed};
    use libc::{c_int, c_void};

    use crate::backend::ops::raw::submission::SubmissionBackend;
    use crate::backend::uring::raw::submission::Submission;
    use crate::backend::uring::sqe::Sqe;

    use super::{Backend, BorrowedFd, ControlBackend, FdSlot, PushError, Token, io};

    impl ControlBackend for Backend {
        fn register_shutdown_fd(backend: &mut Backend, fd: BorrowedFd<'_>) -> io::Result<()> {
            <Backend as SubmissionBackend>::push(backend, Sqe::poll_shutdown(fd.as_raw_fd()))
                .map_err(Error::from)?;
            backend.uring.submit().map(|_| ())
        }

        fn quiesce(backend: &mut Backend, targets: &[Token]) -> bool {
            if targets.is_empty() {
                return false;
            }
            if backend.uring.submit().is_err() {
                abort();
            }
            for target in targets {
                match backend
                    .uring
                    .submitter()
                    .register_sync_cancel(None, CancelBuilder::user_data(target.raw()).all())
                {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(_) => abort(),
                }
            }
            true
        }

        fn submit_option(
            backend: &mut Backend,
            slot: FdSlot,
            level: c_int,
            name: c_int,
            value: c_int,
        ) -> Result<(), PushError> {
            let Ok((key, stored)) = backend.setsockopt.insert_entry(value) else {
                return Err(PushError);
            };
            let optval_ptr = (&raw const *stored).cast::<c_void>();
            let ud = Token::from_key(key);
            let sqe = SetSockOpt::new(
                Fixed(slot.raw()),
                level as u32,
                name as u32,
                optval_ptr,
                size_of::<c_int>() as u32,
            )
            .build()
            .user_data(ud.raw());
            if Submission::push_once(&mut backend.uring, &sqe).is_ok() {
                Ok(())
            } else {
                backend.setsockopt.remove(key);
                Err(PushError)
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use std::io::Error;
    use std::os::fd::AsRawFd;
    use std::ptr::{null, null_mut};

    use libc::{EV_ADD, EV_CLEAR, EVFILT_READ, c_int, kevent, setsockopt, socklen_t, uintptr_t};

    use super::{Backend, BorrowedFd, ControlBackend, FdSlot, PushError, Token, io};
    use crate::backend::kqueue::driver::retry::Retry;
    use crate::backend::kqueue::driver::udata::Udata;

    impl ControlBackend for Backend {
        fn register_shutdown_fd(backend: &mut Backend, fd: BorrowedFd<'_>) -> io::Result<()> {
            let event = kevent {
                ident: fd.as_raw_fd() as uintptr_t,
                filter: EVFILT_READ,
                flags: EV_ADD | EV_CLEAR,
                fflags: 0,
                data: 0,
                udata: Udata::shutdown().into_kevent(),
            };
            let rc = unsafe { kevent(backend.kq.as_raw_fd(), &event, 1, null_mut(), 0, null()) };
            if rc < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(())
            }
        }

        fn quiesce(backend: &mut Backend, targets: &[Token]) -> bool {
            if targets.is_empty() {
                return false;
            }
            for target in targets {
                backend.quiesce_accept(*target);
                backend.quiesce_recv(*target);
                backend.retire_write_token(*target);
            }
            let mut extracted = backend.pending.extract_targets(targets);
            while let Some(completion) = backend.pending.pop_extracted(&mut extracted) {
                backend.reclaim(completion);
            }
            false
        }

        fn submit_option(
            backend: &mut Backend,
            slot: FdSlot,
            level: c_int,
            name: c_int,
            value: c_int,
        ) -> Result<(), PushError> {
            let Some(raw) = backend.raw_fd(slot) else {
                return Err(PushError);
            };
            let rc = unsafe {
                setsockopt(
                    raw,
                    level,
                    name,
                    (&value as *const c_int).cast(),
                    size_of::<c_int>() as socklen_t,
                )
            };
            if rc == 0 { Ok(()) } else { Err(PushError) }
        }
    }
}
