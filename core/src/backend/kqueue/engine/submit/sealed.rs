use std::mem;

use crate::{
    backend::{
        self, bound,
        kqueue::{
            descriptor,
            engine::{event, read, write},
            errno, submission,
        },
    },
    driver::{self, flight},
    io::{fd::handles, socket, transfer},
    platform::reactor,
};

#[repr(transparent)]
pub(crate) struct Queue<'a> {
    backend: &'a mut backend::Kqueue,
}

const _: () =
    assert!(mem::size_of::<Queue<'static>>() == mem::size_of::<&'static mut backend::Kqueue>());

impl<'a> Queue<'a> {
    pub(in crate::backend::kqueue) fn new(backend: &'a mut backend::Kqueue) -> Self {
        Self { backend }
    }
}

impl reactor::Queue for Queue<'_> {
    fn submit<'owner, 'd: 'owner>(
        &mut self,
        submission: bound::Bound<'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        use crate::driver::SubmitError;

        if self.backend.poll.is_failed() || self.backend.pending.is_full() {
            return Err(SubmitError);
        }
        let (submission, reservation) = submission.into_parts();
        let submission = submission.into_inner();
        let ud = reservation.key();
        let accepted = match submission.0 {
            submission::SubmissionInner::AcceptOneshot {
                listener,
                addr_ptr,
                addrlen_ptr,
            } => {
                let Some(raw) = self.backend.files.raw(listener) else {
                    self.backend.push_pending(event::Completion::AcceptFailure {
                        ud,
                        errno: libc::EBADF,
                        more: false,
                    });
                    return Ok(reservation.commit());
                };
                read::Arm::new(self.backend).arm_accept_oneshot_inner(
                    ud,
                    listener,
                    raw,
                    addr_ptr,
                    addrlen_ptr,
                )
            }
            submission::SubmissionInner::AcceptMultishot { listener } => {
                let Some(raw) = self.backend.files.raw(listener) else {
                    self.backend.push_pending(event::Completion::AcceptFailure {
                        ud,
                        errno: libc::EBADF,
                        more: false,
                    });
                    return Ok(reservation.commit());
                };
                read::Arm::new(self.backend).arm_accept_multishot_inner(ud, listener, raw)
            }
            submission::SubmissionInner::RecvMulti { slot } => {
                read::Arm::new(self.backend).arm_recv_multi_inner(ud, slot)
            }
            submission::SubmissionInner::RecvMsgMulti { slot } => {
                read::Arm::new(self.backend).arm_recv_msg_multi_inner(ud, slot)
            }
            submission::SubmissionInner::Send { slot, ptr, len } => {
                self.submit_send_tagged_inner(ud, slot, ptr, len)
            }
            submission::SubmissionInner::SendMsg { slot, msg } => {
                self.submit_send_msg_tagged_inner(ud, slot, msg)
            }
            submission::SubmissionInner::SocketAt { socket, slot } => {
                self.submit_socket_at(socket, slot, ud)
            }
            submission::SubmissionInner::Connect {
                slot,
                addr_ptr,
                addr_len,
            } => self.submit_connect(slot, addr_ptr, addr_len, ud),
        };
        if accepted {
            Ok(reservation.commit())
        } else {
            Err(SubmitError)
        }
    }

    fn cancel(&mut self, flight: &mut flight::Flight<'_>) -> Result<(), driver::SubmitError> {
        self.cancel_inner(flight.key(), flight.target().kind())
            .then_some(())
            .ok_or(driver::SubmitError)
    }
}

impl Queue<'_> {
    pub(in crate::backend::kqueue::engine) fn cancel_inner(
        &mut self,
        target: flight::raw::Echo,
        kind: u8,
    ) -> bool {
        use crate::driver::route::kind;
        match kind {
            kind::ACCEPT => read::Arm::new(self.backend).cancel_accept_inner(target),
            kind::RECV => read::Arm::new(self.backend).cancel_recv_inner(target),
            kind::SEND | kind::CONNECT => {
                write::Retry::new(self.backend).cancel_write_inner(target)
            }
            _ => true,
        }
    }

    pub(in crate::backend::kqueue::engine) fn submit_send_tagged_inner(
        &mut self,
        ud: flight::raw::Echo,
        slot: handles::FixedSlot,
        ptr: *const u8,
        len: transfer::Len,
    ) -> bool {
        let Some(raw) = self.backend.files.raw(slot) else {
            self.backend.push_pending(event::Completion::Send {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        write::Retry::new(self.backend).submit_data_write(raw, ud, write::Data::Send { ptr, len })
    }

    fn complete_create(&mut self, ud: flight::raw::Echo, outcome: event::CreateOutcome) -> bool {
        self.backend
            .push_pending(event::Completion::Create { ud, outcome });
        true
    }

    pub(in crate::backend::kqueue::engine) fn submit_socket_at(
        &mut self,
        spec: socket::StreamSpec,
        slot: handles::FixedSlot,
        ud: flight::raw::Echo,
    ) -> bool {
        if self.backend.files.raw(slot).is_some() || self.backend.pending.has_create(slot) {
            return false;
        }
        let outcome = match descriptor::Handle::open(spec.domain(), socket::Kind::Stream) {
            Ok(sock) => event::CreateOutcome::Ready { slot, fd: sock },
            Err(error) => match error.raw_os_error() {
                Some(errno) => event::CreateOutcome::Failed { slot, errno },
                None => event::CreateOutcome::Failed {
                    slot,
                    errno: libc::EIO,
                },
            },
        };
        self.complete_create(ud, outcome)
    }

    pub(in crate::backend::kqueue::engine) fn submit_connect(
        &mut self,
        slot: handles::FixedSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        ud: flight::raw::Echo,
    ) -> bool {
        use libc::EINPROGRESS;
        let Some(raw) = self.backend.files.raw(slot) else {
            self.backend.push_pending(event::Completion::Connect {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        let rc = unsafe {
            use libc::{connect, socklen_t};
            connect(raw, addr_ptr, addr_len as socklen_t)
        };
        if rc == 0 {
            self.backend
                .push_pending(event::Completion::Connect { ud, result: 0 });
            return true;
        }
        let errno = errno::Errno::last();
        if errno.raw() == EINPROGRESS || errno.is_block() {
            return write::Retry::new(self.backend).arm_write_retry(raw, ud, write::Kind::Connect);
        }
        self.backend.push_pending(event::Completion::Connect {
            ud,
            result: -errno.raw(),
        });
        true
    }

    fn submit_send_msg_tagged_inner(
        &mut self,
        ud: flight::raw::Echo,
        slot: handles::FixedSlot,
        msg: *const libc::msghdr,
    ) -> bool {
        let Some(raw) = self.backend.files.raw(slot) else {
            self.backend.push_pending(event::Completion::Send {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        write::Retry::new(self.backend).submit_data_write(raw, ud, write::Data::SendMsg { msg })
    }
}
