use std::io;
use std::os::fd::RawFd;

use io_uring::{opcode, types};

use crate::backend::socket::{Fd, FdSlot};
use crate::backend::token::{kind, Token};

pub type Timespec = types::Timespec;

#[repr(transparent)]
pub struct Sqe(io_uring::squeue::Entry);

impl Sqe {
    pub(super) fn entry(&self) -> &io_uring::squeue::Entry {
        &self.0
    }

    pub(super) fn from_entry(entry: io_uring::squeue::Entry) -> Self {
        Self(entry)
    }

    pub fn send(fd: &Fd, buf: &[u8], op: Token) -> Self {
        Self::send_at(fd.slot(), buf, op)
    }

    pub(super) fn send_at(slot: FdSlot, buf: &[u8], op: Token) -> Self {
        Self(
            opcode::Send::new(types::Fixed(slot.raw()), buf.as_ptr(), buf.len() as u32)
                .build()
                .user_data(op.with_kind(kind::SEND).raw()),
        )
    }

    pub fn write_fd(fd: RawFd, buf: &[u8], offset: u64, op: Token) -> Self {
        Self(
            opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(op.with_kind(kind::WRITE).raw()),
        )
    }

    pub fn fsync(fd: RawFd, op: Token) -> Self {
        Self(
            opcode::Fsync::new(types::Fd(fd))
                .build()
                .user_data(op.with_kind(kind::SYNC).raw()),
        )
    }

    pub fn recv_multi(fd: &Fd, buf_group: u16, op: Token) -> Self {
        Self(
            opcode::RecvMulti::new(types::Fixed(fd.slot().raw()), buf_group)
                .build()
                .user_data(op.with_kind(kind::RECV).raw()),
        )
    }

    pub fn accept_oneshot(
        listener: &Fd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        op: Token,
    ) -> Self {
        Self(
            opcode::Accept::new(types::Fixed(listener.slot().raw()), addr_ptr, addrlen_ptr)
                .file_index(Some(types::DestinationSlot::auto_target()))
                .flags(0)
                .build()
                .user_data(op.with_kind(kind::ACCEPT).raw()),
        )
    }

    pub fn recv_msg_multi(
        fd: &Fd,
        msghdr: &libc::msghdr,
        buf_group: u16,
        op: Token,
    ) -> Self {
        Self(
            opcode::RecvMsgMulti::new(types::Fixed(fd.slot().raw()), msghdr, buf_group)
                .build()
                .user_data(op.with_kind(kind::RECV).raw()),
        )
    }

    pub fn send_msg(fd: &Fd, msg: &libc::msghdr, op: Token) -> Self {
        Self(
            opcode::SendMsg::new(types::Fixed(fd.slot().raw()), msg)
                .build()
                .user_data(op.with_kind(kind::SEND).raw()),
        )
    }

    pub(super) fn close_at(slot: FdSlot) -> Self {
        Self(
            opcode::Close::new(types::Fixed(slot.raw()))
                .build()
                .user_data(0),
        )
    }

    pub fn quickack(fd: &Fd) -> Self {
        const TCP_QUICKACK: u32 = 12;
        static QUICKACK_ON: libc::c_int = 1;
        Self(
            opcode::SetSockOpt::new(
                types::Fixed(fd.slot().raw()),
                libc::IPPROTO_TCP as u32,
                TCP_QUICKACK,
                &QUICKACK_ON as *const libc::c_int as *const libc::c_void,
                size_of::<libc::c_int>() as u32,
            )
            .build()
            .user_data(0),
        )
    }

    pub fn shutdown(fd: &Fd, how: i32) -> Self {
        Self(
            opcode::Shutdown::new(types::Fixed(fd.slot().raw()), how)
                .build()
                .user_data(0),
        )
    }

    pub(super) fn shutdown_linked_at(slot: FdSlot, how: i32) -> Self {
        Self(
            opcode::Shutdown::new(types::Fixed(slot.raw()), how)
                .build()
                .flags(io_uring::squeue::Flags::IO_HARDLINK)
                .user_data(0),
        )
    }

    pub fn poll_shutdown(fd: RawFd) -> Self {
        Self(
            opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
                .build()
                .user_data(crate::backend::token::SHUTDOWN.raw()),
        )
    }

    pub fn cancel(target: Token, op_kind: u8) -> Self {
        Self(
            opcode::AsyncCancel::new(target.with_kind(op_kind).raw())
                .build()
                .user_data(0),
        )
    }

    pub fn interval(timer: &types::Timespec, op: Token) -> Self {
        Self(
            opcode::Timeout::new(timer)
                .count(0)
                .flags(types::TimeoutFlags::MULTISHOT)
                .build()
                .user_data(op.with_kind(kind::TIMER).raw()),
        )
    }

    pub fn socket(
        domain: i32,
        socket_type: i32,
        protocol: i32,
        fd: &Fd,
        op: Token,
    ) -> io::Result<Self> {
        Self::socket_at(domain, socket_type, protocol, fd.slot(), op)
    }

    pub(super) fn socket_at(
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        op: Token,
    ) -> io::Result<Self> {
        let dest = types::DestinationSlot::try_from_slot_target(slot.raw())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "dope: socket slot out of range"))?;
        Ok(Self(
            opcode::Socket::new(domain, socket_type, protocol)
                .file_index(Some(dest))
                .build()
                .user_data(op.with_kind(kind::SOCKET).raw()),
        ))
    }

    pub(super) fn bind_at(slot: FdSlot, addr_ptr: *const libc::sockaddr, addr_len: u32, op: Token) -> Self {
        Self(
            opcode::Bind::new(types::Fixed(slot.raw()), addr_ptr, addr_len)
                .build()
                .user_data(op.with_kind(kind::SOCKET).raw()),
        )
    }

    pub(super) fn listen_at(slot: FdSlot, backlog: i32, op: Token) -> Self {
        Self(
            opcode::Listen::new(types::Fixed(slot.raw()), backlog)
                .build()
                .user_data(op.with_kind(kind::SOCKET).raw()),
        )
    }

    pub fn connect(fd: &Fd, addr_ptr: *const libc::sockaddr, addr_len: u32, op: Token) -> Self {
        Self(
            opcode::Connect::new(types::Fixed(fd.slot().raw()), addr_ptr, addr_len)
                .build()
                .user_data(op.with_kind(kind::CONNECT).raw()),
        )
    }
}
