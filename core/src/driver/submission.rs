use crate::backend::Sqe;

use super::{DriverContext, PushError};

pub trait Submission {
    fn push(&mut self, sqe: Sqe) -> Result<(), PushError>;
    fn flush_submissions(&mut self) -> bool;
}

cfg_select! {
    target_os = "linux" => {
        use crate::backend::uring::driver::Uring;
        use crate::backend::uring::driver::files::Admission;

        impl Submission for DriverContext<'_, '_> {
            fn push(&mut self, sqe: Sqe) -> Result<(), PushError> {
                let state = self.backend();
                let Some(create) = sqe.create_meta() else {
                    return Uring::entry_push(&mut state.uring, sqe.entry());
                };
                match state.files.admission(create.slot) {
                    Admission::Start => {
                        Uring::entry_push(&mut state.uring, sqe.entry())?;
                        state.files.begin_create(create);
                        Ok(())
                    }
                    Admission::Defer => {
                        state.files.defer_create(create, sqe);
                        Ok(())
                    }
                    Admission::Reject => Err(PushError),
                }
            }

            fn flush_submissions(&mut self) -> bool {
                let state = self.backend();
                state.flush_deferred_close();
                state.flush_ready_create();
                state.uring.submit().is_ok()
            }
        }
    }
    _ => {
        use crate::backend::kqueue::driver::pending::PendingCompletion;
        use crate::backend::kqueue::driver::read::arm::Arm;
        use crate::backend::kqueue::driver::submit::Submit;
        use crate::backend::kqueue::sqe::SqeInner;

        impl Submission for DriverContext<'_, '_> {
            fn push(&mut self, sqe: Sqe) -> Result<(), PushError> {
                let state = self.backend();
                if state.pending.is_full()
                    && !matches!(
                        &sqe.0,
                        SqeInner::Quickack
                            | SqeInner::Shutdown { .. }
                            | SqeInner::Cancel { .. }
                            | SqeInner::CancelCreate { .. }
                    )
                {
                    return Err(PushError);
                }
                let accepted = match sqe.0 {
                    SqeInner::AcceptOneshot {
                        listener,
                        addr_ptr,
                        addrlen_ptr,
                        ud,
                    } => {
                        let Some(raw) = state.raw_fd(listener) else {
                            state.push_pending(PendingCompletion::Accept {
                                ud,
                                result: -libc::EBADF,
                                more: false,
                            });
                            return Ok(());
                        };
                        state.arm_accept_oneshot_inner(ud, raw, addr_ptr, addrlen_ptr)
                    }
                    SqeInner::RecvMulti { slot, ud } => state.arm_recv_multi_inner(ud, slot),
                    SqeInner::RecvMsgMulti { slot, msghdr, ud } => unsafe {
                        state.arm_recv_msg_multi_inner(ud, slot, msghdr)
                    },
                    SqeInner::Send { slot, ptr, len, ud } => {
                        state.submit_send_tagged_inner(ud, slot, ptr, len)
                    }
                    SqeInner::WriteFd {
                        fd,
                        ptr,
                        len,
                        offset,
                        ud,
                    } => state.submit_write_fd_inner(ud, fd, ptr, len, offset),
                    SqeInner::OpenAt {
                        dir,
                        path,
                        flags,
                        mode,
                        ud,
                    } => state.submit_openat_inner(ud, dir, path, flags, mode, None),
                    SqeInner::OpenAtFixed {
                        dir,
                        path,
                        flags,
                        mode,
                        slot,
                        ud,
                    } => state.submit_openat_inner(ud, dir, path, flags, mode, Some(slot)),
                    SqeInner::Read {
                        fd,
                        ptr,
                        len,
                        offset,
                        ud,
                    } => state.submit_read_inner(ud, fd, ptr, len, offset),
                    SqeInner::ReadFixed {
                        slot,
                        ptr,
                        len,
                        offset,
                        ud,
                    } => match state.raw_fd(slot) {
                        Some(fd) => state.submit_read_inner(ud, fd, ptr, len, offset),
                        None => {
                            state.push_pending(PendingCompletion::Write {
                                ud,
                                result: -libc::EBADF,
                            });
                            true
                        }
                    },
                    SqeInner::StatPath { path, stat, ud } => {
                        let rc = unsafe { libc::stat(path, stat) };
                        state.complete_io(ud, rc as isize)
                    }
                    SqeInner::StatFd { fd, stat, ud } => {
                        let rc = unsafe { libc::fstat(fd, stat) };
                        state.complete_io(ud, rc as isize)
                    }
                    SqeInner::Splice {
                        fd_in,
                        off_in,
                        fd_out,
                        off_out,
                        len,
                        ud,
                    } => state.submit_splice_inner(ud, fd_in, off_in, fd_out, off_out, len),
                    SqeInner::SendMsg { slot, msg, ud } => unsafe {
                        state.submit_send_msg_tagged_inner(ud, slot, msg)
                    },
                    SqeInner::Quickack => true,
                    SqeInner::Shutdown { slot, how } => {
                        if let Some(raw) = state.raw_fd(slot) {
                            unsafe { libc::shutdown(raw, how) };
                        }
                        true
                    }
                    SqeInner::Cancel { target } => state.cancel_inner(target),
                    SqeInner::Interval { sec, nsec, ud } => {
                        let micros = (i128::from(sec) * 1_000_000 + i128::from(nsec) / 1_000)
                            .clamp(1, libc::intptr_t::MAX as i128)
                            as libc::intptr_t;
                        state.changes.push(libc::kevent {
                            ident: ud.raw() as libc::uintptr_t,
                            filter: libc::EVFILT_TIMER,
                            flags: libc::EV_ADD,
                            fflags: libc::NOTE_USECONDS,
                            data: micros,
                            udata: ud.raw() as usize as *mut libc::c_void,
                        });
                        state.flush_changes_if_full();
                        true
                    }
                    SqeInner::CancelCreate { slot } => {
                        state.pending.cancel_create(slot);
                        state.close_fd(slot);
                        true
                    }
                    SqeInner::SocketAt {
                        domain,
                        socket_type,
                        protocol,
                        slot,
                        ud,
                    } => state.submit_socket_at(domain, socket_type, protocol, slot, ud),
                    SqeInner::Connect {
                        slot,
                        addr_ptr,
                        addr_len,
                        ud,
                    } => state.submit_connect(slot, addr_ptr, addr_len, ud),
                };
                if accepted {
                    Ok(())
                } else {
                    Err(PushError)
                }
            }

            fn flush_submissions(&mut self) -> bool {
                false
            }
        }
    }
}
