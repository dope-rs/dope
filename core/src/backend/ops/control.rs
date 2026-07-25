use std::io;
use std::os::fd::BorrowedFd;

use crate::backend::Backend;
use crate::driver::token::Token;
use crate::driver::{OutboundReservation, PushError};

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
    fn set(
        backend: &mut Backend,
        fixed_idx: u32,
        level: u32,
        optname: u32,
        value: i32,
    ) -> Result<(), PushError>;
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io::ErrorKind;
    use std::os::fd::AsRawFd;
    use std::process::abort;

    use io_uring::opcode::SetSockOpt;
    use io_uring::types::{CancelBuilder, Fixed};

    use crate::backend::ops::submission::SubmissionBackend;
    use crate::backend::uring::sqe::Sqe;

    use super::{Backend, BorrowedFd, ControlBackend, PushError, Token, io};

    impl ControlBackend for Backend {
        fn register_shutdown_fd(backend: &mut Backend, fd: BorrowedFd<'_>) -> io::Result<()> {
            <Backend as SubmissionBackend>::push(backend, Sqe::poll_shutdown(fd.as_raw_fd()))
                .map_err(io::Error::from)?;
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

        fn set(
            backend: &mut Backend,
            fixed_idx: u32,
            level: u32,
            optname: u32,
            value: i32,
        ) -> Result<(), PushError> {
            let Ok((key, stored)) = backend.setsockopt.insert_entry(value) else {
                return Err(PushError);
            };
            let optval_ptr = (&raw const *stored).cast::<libc::c_void>();
            let ud = Token::from_key(key);
            let sqe = SetSockOpt::new(
                Fixed(fixed_idx),
                level,
                optname,
                optval_ptr,
                size_of::<libc::c_int>() as u32,
            )
            .build()
            .user_data(ud.raw());
            if unsafe { backend.uring.submission().push(&sqe) }.is_ok() {
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
    use std::os::fd::AsRawFd;
    use std::ptr::{null, null_mut};

    use crate::backend::kqueue::driver::retry::Retry;
    use crate::backend::kqueue::driver::udata::Udata;
    use crate::io::fd::FdSlot;

    use super::{Backend, BorrowedFd, ControlBackend, PushError, Token, io};

    impl ControlBackend for Backend {
        fn register_shutdown_fd(
            backend: &mut Backend,
            fd: BorrowedFd<'_>,
        ) -> io::Result<()> {
            let event = libc::kevent {
                ident: fd.as_raw_fd() as libc::uintptr_t,
                filter: libc::EVFILT_READ,
                flags: libc::EV_ADD | libc::EV_CLEAR,
                fflags: 0,
                data: 0,
                udata: Udata::pack(crate::backend::kqueue::driver::TAG_SHUTDOWN, 0, 0)
                    .into_kevent(),
            };
            let rc =
                unsafe { libc::kevent(backend.kq.as_raw_fd(), &event, 1, null_mut(), 0, null()) };
            if rc < 0 {
                Err(io::Error::last_os_error())
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

        fn set(
            backend: &mut Backend,
            fixed_idx: u32,
            level: u32,
            optname: u32,
            value: i32,
        ) -> Result<(), PushError> {
            let Some(raw) = backend.raw_fd(FdSlot::new(fixed_idx)) else {
                return Err(PushError);
            };
            let rc = unsafe {
                libc::setsockopt(
                    raw,
                    level as libc::c_int,
                    optname as libc::c_int,
                    (&value as *const libc::c_int).cast(),
                    size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            if rc == 0 { Ok(()) } else { Err(PushError) }
        }
    }
}
