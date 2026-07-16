use std::io;
use std::os::fd::BorrowedFd;

use super::token::Token;
use super::{DriverContext, OutboundReservation, PushError};

pub trait ContextControl {
    fn prepare_drop(&mut self);
    /// # Safety
    /// `fd` must remain open through this call.
    unsafe fn register_shutdown_fd(&mut self, fd: BorrowedFd<'_>) -> io::Result<()>;
    fn reserve_outbound(&mut self, count: u32) -> io::Result<OutboundReservation>;
    fn reserve_route(&mut self, id: u8) -> bool;
    fn release_route(&mut self, id: u8);
    fn poison_route(&mut self, id: u8);
    fn quiesce(&mut self, targets: &[Token]) -> bool;
    fn set(
        &mut self,
        fixed_idx: u32,
        level: u32,
        optname: u32,
        value: i32,
    ) -> Result<(), PushError>;
}

cfg_select! {
    target_os = "linux" => {
        use std::io::ErrorKind;
        use std::mem::size_of;
        use std::os::fd::AsRawFd;
        use std::process::abort;

        use io_uring::opcode::SetSockOpt;
        use io_uring::types::{CancelBuilder, Fixed};

        use super::submission::Submission;
        use crate::backend::uring::sqe::Sqe;

        impl ContextControl for DriverContext<'_, '_> {
            fn prepare_drop(&mut self) {
                self.backend().shutdown();
            }

            /// # Safety
            /// `fd` must remain open through this call.
            unsafe fn register_shutdown_fd(&mut self, fd: BorrowedFd<'_>) -> io::Result<()> {
                Submission::push(self, Sqe::poll_shutdown(fd.as_raw_fd())).map_err(io::Error::from)?;
                self.backend().uring.submit().map(|_| ())
            }

            fn reserve_outbound(&mut self, count: u32) -> io::Result<OutboundReservation> {
                let base = self.backend().alloc_fixed_range(count)?;
                Ok(OutboundReservation::new(base, count))
            }

            fn reserve_route(&mut self, id: u8) -> bool {
                self.backend().routes.reserve(id)
            }

            fn release_route(&mut self, id: u8) {
                self.backend().routes.release(id);
            }

            fn poison_route(&mut self, id: u8) {
                self.backend().routes.poison(id);
            }

            fn quiesce(&mut self, targets: &[Token]) -> bool {
                if targets.is_empty() {
                    return false;
                }
                let state = self.backend();
                if state.uring.submit().is_err() {
                    abort();
                }
                for target in targets {
                    match state
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
                &mut self,
                fixed_idx: u32,
                level: u32,
                optname: u32,
                value: i32,
            ) -> Result<(), PushError> {
                let state = self.backend();
                let Ok((key, stored)) = state.setsockopt.insert_entry(value) else {
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
                if unsafe { state.uring.submission().push(&sqe) }.is_ok() {
                    Ok(())
                } else {
                    state.setsockopt.remove(key);
                    Err(PushError)
                }
            }
        }
    }
    _ => {
        use std::mem::size_of;
        use std::os::fd::AsRawFd;
        use std::ptr::{null, null_mut};

        use crate::backend::kqueue::driver::TAG_SHUTDOWN;
        use crate::backend::kqueue::driver::retry::Retry;
        use crate::backend::kqueue::driver::udata::Udata;
        use crate::io::fd::FdSlot;

        impl ContextControl for DriverContext<'_, '_> {
            fn prepare_drop(&mut self) {
                self.backend().shutdown();
            }

            /// # Safety
            /// `fd` must remain open through this call.
            unsafe fn register_shutdown_fd(&mut self, fd: BorrowedFd<'_>) -> io::Result<()> {
                let state = self.backend();
                let event = libc::kevent {
                    ident: fd.as_raw_fd() as libc::uintptr_t,
                    filter: libc::EVFILT_READ,
                    flags: libc::EV_ADD | libc::EV_CLEAR,
                    fflags: 0,
                    data: 0,
                    udata: Udata::pack(TAG_SHUTDOWN, 0, 0).into_kevent(),
                };
                let rc = unsafe {
                    libc::kevent(
                        state.kq.as_raw_fd(),
                        &event,
                        1,
                        null_mut(),
                        0,
                        null(),
                    )
                };
                if rc < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            }

            fn reserve_outbound(&mut self, count: u32) -> io::Result<OutboundReservation> {
                let base = self.backend().alloc_fixed_range(count)?;
                Ok(OutboundReservation::new(base, count))
            }

            fn reserve_route(&mut self, id: u8) -> bool {
                self.backend().routes.reserve(id)
            }

            fn release_route(&mut self, id: u8) {
                self.backend().routes.release(id);
            }

            fn poison_route(&mut self, id: u8) {
                self.backend().routes.poison(id);
            }

            fn quiesce(&mut self, targets: &[Token]) -> bool {
                if targets.is_empty() {
                    return false;
                }
                let state = self.backend();
                for target in targets {
                    state.quiesce_accept(*target);
                    state.quiesce_recv(*target);
                    state.retire_write_token(*target);
                }
                let mut extracted = state.pending.extract_targets(targets);
                while let Some(completion) = state.pending.pop_extracted(&mut extracted) {
                    state.reclaim(completion);
                }
                false
            }

            fn set(
                &mut self,
                fixed_idx: u32,
                level: u32,
                optname: u32,
                value: i32,
            ) -> Result<(), PushError> {
                let Some(raw) = self.backend_ref().raw_fd(FdSlot::new(fixed_idx)) else {
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
}
