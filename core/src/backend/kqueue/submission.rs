use crate::{
    backend::{self, operations},
    io::{fd::handles, socket, transfer},
};

pub(in crate::backend::kqueue) enum SubmissionInner {
    Send {
        slot: handles::FixedSlot,
        ptr: *const u8,
        len: transfer::Len,
    },
    SendMsg {
        slot: handles::FixedSlot,
        msg: *const libc::msghdr,
    },
    AcceptOneshot {
        listener: handles::FixedSlot,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
    },
    AcceptMultishot {
        listener: handles::FixedSlot,
    },
    RecvMulti {
        slot: handles::FixedSlot,
    },
    RecvMsgMulti {
        slot: handles::FixedSlot,
    },
    SocketAt {
        socket: socket::StreamSpec,
        slot: handles::FixedSlot,
    },
    Connect {
        slot: handles::FixedSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
    },
}

pub(in crate::backend::kqueue) struct Submission(
    pub(in crate::backend::kqueue) SubmissionInner,
    o3::ThreadBound,
);

/// A submission borrowing caller-owned resources through completion.
#[repr(transparent)]
pub(crate) struct RawSubmission(Submission);

impl Submission {
    fn new(inner: SubmissionInner) -> Self {
        use o3::ThreadBound;
        Self(inner, ThreadBound::NEW)
    }
}

impl RawSubmission {
    pub(in crate::backend::kqueue) fn new(inner: SubmissionInner) -> Self {
        Self(Submission::new(inner))
    }

    pub(in crate::backend::kqueue) fn into_inner(self) -> Submission {
        self.0
    }
}

impl backend::raw::Lower for RawSubmission {
    fn socket(op: backend::raw::Prepared<operations::Socket<'_>>) -> RawSubmission {
        let operations::Socket { slot, socket } = op.into_inner();
        Self::new(SubmissionInner::SocketAt {
            socket,
            slot: *slot,
        })
    }

    fn send(op: backend::raw::Prepared<operations::Send<'_>>) -> RawSubmission {
        let operations::Send { slot, buffer, len } = op.into_inner();
        Self::new(SubmissionInner::Send {
            slot: *slot,
            ptr: buffer.as_ptr(),
            len,
        })
    }

    fn send_msg(op: backend::raw::Prepared<operations::SendMsg<'_>>) -> RawSubmission {
        let operations::SendMsg { slot, message } = op.into_inner();
        Self::new(SubmissionInner::SendMsg {
            slot: *slot,
            msg: message.raw(),
        })
    }

    fn accept_oneshot(op: backend::raw::Prepared<operations::AcceptOneshot<'_>>) -> RawSubmission {
        let operations::AcceptOneshot { listener, peer } = op.into_inner();
        Self::new(SubmissionInner::AcceptOneshot {
            listener: *listener,
            addr_ptr: peer.mut_ptr(),
            addrlen_ptr: peer.len_ptr(),
        })
    }

    fn accept_multishot(
        op: backend::raw::Prepared<operations::AcceptMultishot<'_>>,
    ) -> RawSubmission {
        let operations::AcceptMultishot { listener } = op.into_inner();
        Self::new(SubmissionInner::AcceptMultishot {
            listener: *listener,
        })
    }

    fn recv(op: backend::raw::Prepared<operations::Recv<'_>>) -> RawSubmission {
        let operations::Recv { slot } = op.into_inner();
        Self::new(SubmissionInner::RecvMulti { slot: *slot })
    }

    fn recv_message(op: backend::raw::Prepared<operations::RecvMsgMulti<'_>>) -> RawSubmission {
        let operations::RecvMsgMulti { slot } = op.into_inner();
        Self::new(SubmissionInner::RecvMsgMulti { slot: *slot })
    }

    fn connect(op: backend::raw::Prepared<operations::Connect<'_>>) -> RawSubmission {
        let operations::Connect { slot, addr } = op.into_inner();
        let addr = addr.raw();
        Self::new(SubmissionInner::Connect {
            slot,
            addr_ptr: addr.ptr(),
            addr_len: addr.socklen(),
        })
    }
}
