use std::io;
use std::os::fd::BorrowedFd;

use libc::c_int;

use crate::backend::Backend;
use crate::driver::PushError;
use crate::driver::token::Token;
use crate::io::fd::FdSlot;

#[cfg(target_os = "linux")]
pub(crate) struct RawQuiesce {
    started: bool,
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct RawQuiesce {
    extracted: crate::backend::kqueue::driver::pending::Extracted,
}

pub(crate) trait ControlBackend {
    fn prepare_drop(backend: &mut Backend) {
        backend.shutdown();
    }
    fn register_shutdown_fd(backend: &mut Backend, fd: BorrowedFd<'_>) -> io::Result<()>;
    fn reserve_outbound(backend: &mut Backend, count: u32) -> io::Result<u32> {
        backend.alloc_fixed_range(count)
    }
    fn retire_fixed(backend: &mut Backend, base: u32, count: u32) {
        backend.retire_fixed_range(base, count);
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
    fn begin_quiesce() -> RawQuiesce;
    fn quiesce_target(backend: &mut Backend, state: &mut RawQuiesce, target: Token);
    fn finish_quiesce(backend: &mut Backend, state: RawQuiesce) -> bool;
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

    use io_uring::types::CancelBuilder;
    use libc::c_int;

    use super::{Backend, BorrowedFd, ControlBackend, FdSlot, PushError, RawQuiesce, Token, io};
    use crate::backend::ops::raw::submission::SubmissionBackend;
    use crate::backend::uring::raw::submission::Submission;
    use crate::backend::{RawSqe, RetainedSqe, Sqe, StableSqeSource};

    struct SocketOptionSubmission(RawSqe);

    // SAFETY: the backend's setsockopt slab owns the pointed-to value until
    // terminal completion, failed submission, or ring quiescence.
    unsafe impl StableSqeSource for SocketOptionSubmission {
        fn into_raw(self) -> RawSqe {
            self.0
        }
    }

    impl ControlBackend for Backend {
        fn register_shutdown_fd(backend: &mut Backend, fd: BorrowedFd<'_>) -> io::Result<()> {
            <Backend as SubmissionBackend>::push(backend, Sqe::poll_shutdown(fd.as_raw_fd()))
                .map_err(Error::from)?;
            backend.ring.io_mut().submit().map(|_| ())
        }

        fn begin_quiesce() -> RawQuiesce {
            RawQuiesce { started: false }
        }

        fn quiesce_target(backend: &mut Backend, state: &mut RawQuiesce, target: Token) {
            if !state.started {
                if backend.ring.io_mut().submit().is_err() {
                    abort();
                }
                state.started = true;
            }
            match backend
                .ring
                .io()
                .submitter()
                .register_sync_cancel(None, CancelBuilder::user_data(target.raw()).all())
            {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(_) => abort(),
            }
        }

        fn finish_quiesce(_backend: &mut Backend, state: RawQuiesce) -> bool {
            state.started
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
            let sqe = RawSqe::setsockopt_at(slot, level, name, stored, Token::from_key(key));
            let sqe = Sqe::from_retained(RetainedSqe::from_stable(SocketOptionSubmission(sqe)));
            if Submission::push_once(backend.ring.io_mut(), &sqe).is_ok() {
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

    use super::{Backend, BorrowedFd, ControlBackend, FdSlot, PushError, RawQuiesce, Token, io};
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

        fn begin_quiesce() -> RawQuiesce {
            RawQuiesce {
                extracted: crate::backend::kqueue::driver::pending::Extracted::new(),
            }
        }

        fn quiesce_target(backend: &mut Backend, state: &mut RawQuiesce, target: Token) {
            backend.quiesce_accept(target);
            backend.quiesce_recv(target);
            backend.retire_write_token(target);
            backend.pending.extract_target(target, &mut state.extracted);
        }

        fn finish_quiesce(backend: &mut Backend, mut state: RawQuiesce) -> bool {
            while let Some(completion) = backend.pending.pop_extracted(&mut state.extracted) {
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
