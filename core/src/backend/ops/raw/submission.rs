use crate::backend::{Backend, Sqe};
use crate::driver::PushError;

pub(crate) trait SubmissionBackend {
    fn push(backend: &mut Backend, sqe: Sqe) -> Result<(), PushError>;
    fn flush_submissions(backend: &mut Backend) -> bool;
}

#[cfg(target_os = "linux")]
mod linux {
    use crate::backend::uring::driver::files::Admission;
    use crate::backend::uring::raw::submission::Submission;

    use super::{Backend, PushError, Sqe, SubmissionBackend};

    impl SubmissionBackend for Backend {
        fn push(backend: &mut Backend, sqe: Sqe) -> Result<(), PushError> {
            let Some(create) = sqe.create_meta() else {
                return Submission::push(&mut backend.uring, sqe.entry());
            };
            match backend.files.admission(create.slot) {
                Admission::Start => {
                    Submission::push(&mut backend.uring, sqe.entry())?;
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
    use libc::{
        EBADF, EV_ADD, EVFILT_TIMER, NOTE_USECONDS, c_void, fstat, intptr_t, kevent, stat,
        uintptr_t,
    };

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
                    SqeInner::Quickack | SqeInner::Cancel { .. } | SqeInner::CancelCreate { .. }
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
                            result: -EBADF,
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
                SqeInner::StatPath {
                    path,
                    stat: output,
                    ud,
                } => {
                    let rc = unsafe { stat(path, output) };
                    backend.complete_io(ud, rc as isize)
                }
                SqeInner::StatFd { fd, stat, ud } => {
                    let rc = unsafe { fstat(fd, stat) };
                    backend.complete_io(ud, rc as isize)
                }
                SqeInner::SendMsg { slot, msg, ud } => unsafe {
                    backend.submit_send_msg_tagged_inner(ud, slot, msg)
                },
                SqeInner::Quickack => true,
                SqeInner::Cancel { target } => backend.cancel_inner(target),
                SqeInner::Interval { sec, nsec, ud } => {
                    let micros = (i128::from(sec) * 1_000_000 + i128::from(nsec) / 1_000)
                        .clamp(1, intptr_t::MAX as i128)
                        as intptr_t;
                    backend.changes.push(kevent {
                        ident: ud.raw() as uintptr_t,
                        filter: EVFILT_TIMER,
                        flags: EV_ADD,
                        fflags: NOTE_USECONDS,
                        data: micros,
                        udata: ud.raw() as usize as *mut c_void,
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
