mod sealed;

use std::{mem, os::fd};

use io_uring::{opcode, squeue, types};

use crate::{
    backend::{
        self, operations,
        uring::{self, engine::controls},
    },
    driver::flight,
    io::fd::handles,
};

pub(in crate::backend::uring) struct Submission {
    entry: squeue::Entry,
    _thread: o3::ThreadBound,
}

#[repr(transparent)]
pub(in crate::backend::uring) struct Bound(Submission);

/// A submission borrowing caller-owned resources through completion.
#[repr(transparent)]
pub(crate) struct RawSubmission(Submission);

const _: () = {
    assert!(mem::size_of::<Submission>() == mem::size_of::<squeue::Entry>());
    assert!(mem::size_of::<Bound>() == mem::size_of::<squeue::Entry>());
    assert!(mem::size_of::<RawSubmission>() == mem::size_of::<squeue::Entry>());
};

impl Submission {
    pub(in crate::backend::uring) fn entry(&self) -> &squeue::Entry {
        &self.entry
    }

    pub(in crate::backend::uring) fn into_entry(self) -> squeue::Entry {
        self.entry
    }

    fn new(entry: squeue::Entry) -> Self {
        Self {
            entry,
            _thread: o3::ThreadBound::NEW,
        }
    }

    fn bind(self, key: flight::raw::Echo) -> Bound {
        Bound(Self::new(self.entry.user_data(key.raw())))
    }
}

impl Bound {
    pub(in crate::backend::uring) fn entry(&self) -> &squeue::Entry {
        self.0.entry()
    }
}

impl RawSubmission {
    const BUFFER_GROUP: u16 = { uring::ffi::ProvidedRing::GROUP_ID };

    fn new(entry: squeue::Entry) -> Self {
        Self(Submission::new(entry))
    }

    pub(in crate::backend::uring) fn into_inner(self) -> Submission {
        self.0
    }

    pub(in crate::backend::uring) fn bind(self, key: flight::raw::Echo) -> Bound {
        self.0.bind(key)
    }
}

impl backend::raw::Lower for RawSubmission {
    fn socket(op: backend::raw::Prepared<operations::Socket<'_>>) -> RawSubmission {
        use io_uring::opcode::Socket;

        let operations::Socket { slot, socket } = op.into_inner();
        let Ok(dest) = types::DestinationSlot::try_from_slot_target(slot.raw()) else {
            use std::process::abort;
            abort();
        };
        Self::new(
            Socket::new(socket.domain().raw(), libc::SOCK_STREAM, 0)
                .file_index(Some(dest))
                .build(),
        )
    }

    fn send(op: backend::raw::Prepared<operations::Send<'_>>) -> RawSubmission {
        let operations::Send { slot, buffer, len } = op.into_inner();
        Self::new(
            opcode::Send::new(types::Fixed(slot.raw()), buffer.as_ptr(), len.get())
                .flags(libc::MSG_NOSIGNAL)
                .build(),
        )
    }

    fn send_msg(op: backend::raw::Prepared<operations::SendMsg<'_>>) -> RawSubmission {
        let operations::SendMsg { slot, message } = op.into_inner();
        Self::new(
            opcode::SendMsg::new(types::Fixed(slot.raw()), message.raw())
                .flags(libc::MSG_NOSIGNAL.unsigned_abs())
                .build(),
        )
    }

    fn accept_oneshot(op: backend::raw::Prepared<operations::AcceptOneshot<'_>>) -> RawSubmission {
        let operations::AcceptOneshot { listener, peer } = op.into_inner();
        let addr = peer.mut_ptr();
        let addr_len = peer.len_ptr();
        Self::new(
            opcode::Accept::new(types::Fixed(listener.raw()), addr, addr_len)
                .file_index(Some(types::DestinationSlot::auto_target()))
                .flags(0)
                .build(),
        )
    }

    fn accept_multishot(
        op: backend::raw::Prepared<operations::AcceptMultishot<'_>>,
    ) -> RawSubmission {
        let operations::AcceptMultishot { listener } = op.into_inner();
        Self::new(
            opcode::AcceptMulti::new(types::Fixed(listener.raw()))
                .allocate_file_index(true)
                .flags(0)
                .build(),
        )
    }

    fn recv(op: backend::raw::Prepared<operations::Recv<'_>>) -> RawSubmission {
        let operations::Recv { slot } = op.into_inner();
        Self::new(opcode::RecvMulti::new(types::Fixed(slot.raw()), Self::BUFFER_GROUP).build())
    }

    fn recv_message(op: backend::raw::Prepared<operations::RecvMsgMulti<'_>>) -> RawSubmission {
        use uring::ffi::recvmsg::header;

        let operations::RecvMsgMulti { slot } = op.into_inner();
        Self::new(
            opcode::RecvMsgMulti::new(
                types::Fixed(slot.raw()),
                header::Header::datagram(),
                Self::BUFFER_GROUP,
            )
            .build(),
        )
    }

    fn connect(op: backend::raw::Prepared<operations::Connect<'_>>) -> RawSubmission {
        let operations::Connect { slot, addr } = op.into_inner();
        let addr = addr.raw();
        Self::new(
            opcode::Connect::new(types::Fixed(slot.raw()), addr.ptr(), addr.socklen()).build(),
        )
    }
}

impl Submission {
    pub(crate) fn close_at(slot: handles::FixedSlot) -> Self {
        use io_uring::opcode::Close;

        Self::new(
            Close::new(types::Fixed(slot.raw())).build().user_data(
                controls::Close::new(slot.token_index())
                    .token()
                    .framework_raw(),
            ),
        )
    }

    pub(in crate::backend::uring) fn retire_at(slot: handles::FixedSlot) -> Self {
        use io_uring::opcode::Close;

        Self::new(
            Close::new(types::Fixed(slot.raw())).build().user_data(
                controls::Retire::new(slot.token_index())
                    .token()
                    .framework_raw(),
            ),
        )
    }

    pub(crate) fn shutdown_linked_at(slot: handles::FixedSlot, how: i32) -> Self {
        use io_uring::{opcode::Shutdown, squeue::Flags};

        Self::new(
            Shutdown::new(types::Fixed(slot.raw()), how)
                .build()
                .flags(Flags::IO_HARDLINK | Flags::SKIP_SUCCESS)
                .user_data(
                    controls::Close::new(slot.token_index())
                        .prepare()
                        .framework_raw(),
                ),
        )
    }

    pub(crate) fn poll_shutdown(fd: fd::RawFd) -> Self {
        use io_uring::opcode::PollAdd;
        use libc::POLLIN;

        use crate::driver::route::SHUTDOWN;
        Self::new(
            PollAdd::new(types::Fd(fd), u32::from(POLLIN.unsigned_abs()))
                .build()
                .user_data(SHUTDOWN.framework_raw()),
        )
    }

    pub(crate) fn cancel(target: flight::raw::Echo) -> Self {
        Self::new(
            opcode::AsyncCancel::new(target.raw())
                .build()
                .flags(squeue::Flags::SKIP_SUCCESS)
                .user_data(0),
        )
    }
}
