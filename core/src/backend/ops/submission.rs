use crate::backend::{Backend, Sqe};
use crate::driver::PushError;

pub(crate) trait SubmissionBackend {
    fn push(backend: &mut Backend, sqe: Sqe) -> Result<(), PushError>;
    fn flush_submissions(backend: &mut Backend) -> bool;
}

#[cfg(target_os = "linux")]
mod linux {
    use crate::backend::uring::driver::files::Admission;

    use super::{Backend, PushError, Sqe, SubmissionBackend};

    impl SubmissionBackend for Backend {
        fn push(backend: &mut Backend, sqe: Sqe) -> Result<(), PushError> {
            let Some(create) = sqe.create_meta() else {
                return Backend::entry_push(&mut backend.uring, sqe.entry());
            };
            match backend.files.admission(create.slot) {
                Admission::Start => {
                    Backend::entry_push(&mut backend.uring, sqe.entry())?;
                    backend.files.begin_create(create);
                    Ok(())
                }
                Admission::Defer => {
                    backend.files.defer_create(create, sqe);
                    Ok(())
                }
                Admission::Reject => Err(PushError),
            }
        }

        fn flush_submissions(backend: &mut Backend) -> bool {
            backend.flush_deferred_close();
            backend.flush_ready_create();
            backend.uring.submit().is_ok()
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use crate::backend::kqueue::driver::pending::PendingCompletion;
    use crate::backend::kqueue::driver::read::arm::Arm;
    use crate::backend::kqueue::driver::submit::Submit;
    use crate::backend::kqueue::sqe::SqeInner;

    use super::{Backend, PushError, Sqe, SubmissionBackend};

    impl SubmissionBackend for Backend {
        fn push(backend: &mut Backend, sqe: Sqe) -> Result<(), PushError> {
            if backend.pending.is_full()
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
                    let Some(raw) = backend.raw_fd(listener) else {
                        backend.push_pending(PendingCompletion::Accept {
                            ud,
                            result: -libc::EBADF,
                            more: false,
                        });
                        return Ok(());
                    };
                    backend.arm_accept_oneshot_inner(ud, raw, addr_ptr, addrlen_ptr)
                }
                SqeInner::RecvMulti { slot, ud } => backend.arm_recv_multi_inner(ud, slot),
                SqeInner::RecvMsgMulti { slot, msghdr, ud } => unsafe {
                    backend.arm_recv_msg_multi_inner(ud, slot, msghdr)
                },
                SqeInner::Send { slot, ptr, len, ud } => {
                    backend.submit_send_tagged_inner(ud, slot, ptr, len)
                }
                SqeInner::WriteFd {
                    fd,
                    ptr,
                    len,
                    offset,
                    ud,
                } => backend.submit_write_fd_inner(ud, fd, ptr, len, offset),
                SqeInner::OpenAt {
                    dir,
                    path,
                    flags,
                    mode,
                    ud,
                } => backend.submit_openat_inner(ud, dir, path, flags, mode),
                SqeInner::Read {
                    fd,
                    ptr,
                    len,
                    offset,
                    ud,
                } => backend.submit_read_inner(ud, fd, ptr, len, offset),
                SqeInner::StatPath { path, stat, ud } => {
                    let rc = unsafe { libc::stat(path, stat) };
                    backend.complete_io(ud, rc as isize)
                }
                SqeInner::StatFd { fd, stat, ud } => {
                    let rc = unsafe { libc::fstat(fd, stat) };
                    backend.complete_io(ud, rc as isize)
                }
                SqeInner::SendMsg { slot, msg, ud } => unsafe {
                    backend.submit_send_msg_tagged_inner(ud, slot, msg)
                },
                SqeInner::Quickack => true,
                SqeInner::Shutdown { slot, how } => {
                    if let Some(raw) = backend.raw_fd(slot) {
                        unsafe { libc::shutdown(raw, how) };
                    }
                    true
                }
                SqeInner::Cancel { target } => backend.cancel_inner(target),
                SqeInner::Interval { sec, nsec, ud } => {
                    let micros = (i128::from(sec) * 1_000_000 + i128::from(nsec) / 1_000)
                        .clamp(1, libc::intptr_t::MAX as i128)
                        as libc::intptr_t;
                    backend.changes.push(libc::kevent {
                        ident: ud.raw() as libc::uintptr_t,
                        filter: libc::EVFILT_TIMER,
                        flags: libc::EV_ADD,
                        fflags: libc::NOTE_USECONDS,
                        data: micros,
                        udata: ud.raw() as usize as *mut libc::c_void,
                    });
                    backend.flush_changes_if_full();
                    true
                }
                SqeInner::CancelCreate { slot } => {
                    backend.pending.cancel_create(slot);
                    backend.close_fd(slot);
                    true
                }
                SqeInner::SocketAt {
                    domain,
                    socket_type,
                    protocol,
                    slot,
                    ud,
                } => backend.submit_socket_at(domain, socket_type, protocol, slot, ud),
                SqeInner::Connect {
                    slot,
                    addr_ptr,
                    addr_len,
                    ud,
                } => backend.submit_connect(slot, addr_ptr, addr_len, ud),
            };
            if accepted { Ok(()) } else { Err(PushError) }
        }

        fn flush_submissions(_backend: &mut Backend) -> bool {
            false
        }
    }
}
