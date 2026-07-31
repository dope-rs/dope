use std::io::{self, Error, ErrorKind};
use std::os::fd::RawFd;

use io_uring::types;
use o3::marker::ThreadBound;

use crate::driver::token::SHUTDOWN;
use crate::driver::token::Token;
use crate::io::fd::{Fd, FdSlot};
use io_uring::opcode::Accept;
use io_uring::opcode::AsyncCancel;
use io_uring::opcode::Bind;
use io_uring::opcode::Close;
use io_uring::opcode::Connect;
use io_uring::opcode::Listen;
use io_uring::opcode::OpenAt;
use io_uring::opcode::PollAdd;
use io_uring::opcode::Read;
use io_uring::opcode::Recv;
use io_uring::opcode::RecvMsgMulti;
use io_uring::opcode::RecvMulti;
use io_uring::opcode::Send;
use io_uring::opcode::SendMsg;
use io_uring::opcode::SetSockOpt;
use io_uring::opcode::Shutdown;
use io_uring::opcode::Socket;
use io_uring::opcode::Statx;
use io_uring::opcode::Timeout;
use io_uring::opcode::Write;
use io_uring::squeue::Entry;
use io_uring::squeue::Flags;
use io_uring::types::DestinationSlot;
use crate::driver::token::kind::ACCEPT;
use libc::AT_EMPTY_PATH;
use libc::AT_FDCWD;
use crate::driver::token::kind::CLOSE;
use crate::driver::token::kind::CLOSE_PREP;
use crate::driver::token::kind::CONNECT;
use crate::driver::token::kind::CREATE;
use io_uring::types::Fixed;
use libc::IPPROTO_TCP;
use libc::MSG_NOSIGNAL;
use libc::MSG_TRUNC;
use crate::driver::token::kind::OPEN;
use libc::POLLIN;
use crate::driver::token::kind::READ;
use crate::driver::token::kind::RECV;
use crate::driver::token::kind::RECV_DISCARD;
use crate::driver::token::kind::SEND;
use crate::driver::token::kind::SOCKET;
use crate::driver::token::kind::STAT;
use libc::STATX_MTIME;
use libc::STATX_SIZE;
use libc::STATX_TYPE;
use crate::driver::token::kind::TIMER;
use io_uring::types::TimeoutFlags;
use io_uring::types::Timespec;
use crate::driver::token::kind::WRITE;
use libc::c_char;
use libc::c_int;
use libc::c_void;
use libc::mode_t;
use libc::msghdr;
use libc::sockaddr;
use libc::socklen_t;
use libc::statx;

use crate::backend::RetainedSqe;

#[derive(Clone, Copy)]
pub(crate) struct Create {
    pub(crate) slot: FdSlot,
    pub(crate) token: Token,
}

pub struct Sqe {
    entry: Entry,
    create: Option<Create>,
    _thread: ThreadBound,
}

/// A submission borrowing caller-owned resources through completion.
#[repr(transparent)]
pub struct RawSqe(Sqe);

impl Sqe {
    pub(in crate::backend::uring) fn entry(&self) -> &Entry {
        &self.entry
    }

    pub(crate) fn from_retained(retained: RetainedSqe) -> Self {
        retained.0.0
    }

    pub(crate) fn create_meta(&self) -> Option<Create> {
        self.create
    }

    fn new(entry: Entry) -> Self {
        Self {
            entry,
            create: None,
            _thread: ThreadBound::NEW,
        }
    }

    fn create(entry: Entry, slot: FdSlot, token: Token) -> Self {
        let framework_token = Token::framework(slot.token_index()).with_kind(CREATE);
        Self {
            entry: entry.user_data(framework_token.raw()),
            create: Some(Create { slot, token }),
            _thread: ThreadBound::NEW,
        }
    }

    fn framework(slot: FdSlot, op_kind: u8) -> u64 {
        Token::framework(slot.token_index()).with_kind(op_kind)
            .raw()
    }

}
impl RawSqe {
    fn new(entry: Entry) -> Self {
        Self(Sqe::new(entry))
    }

    pub(crate) fn from_entry(entry: Entry) -> Self {
        Self::new(entry)
    }

    pub(crate) fn setsockopt_at(
        slot: FdSlot,
        level: c_int,
        name: c_int,
        value: &c_int,
        op: Token,
    ) -> Self {
        Self::new(
            SetSockOpt::new(
                Fixed(slot.raw()),
                level as u32,
                name as u32,
                (value as *const c_int).cast::<c_void>(),
                size_of::<c_int>() as u32,
            )
            .build()
            .user_data(op.raw()),
        )
    }

    pub fn send(fd: &Fd, buf: &[u8], op: Token) -> Self {
        Self::send_at(fd.slot(), buf, op)
    }

    fn send_at(slot: FdSlot, buf: &[u8], op: Token) -> Self {
        Self::new(
            Send::new(Fixed(slot.raw()), buf.as_ptr(), buf.len() as u32)
                .flags(MSG_NOSIGNAL)
                .build()
                .user_data(op.with_kind(SEND).raw()),
        )
    }

    pub fn write_fd(fd: RawFd, buf: &[u8], offset: u64, op: Token) -> Self {
        Self::new(
            Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(op.with_kind(WRITE).raw()),
        )
    }

    pub fn openat(dir: RawFd, path: *const c_char, flags: i32, mode: u32, op: Token) -> Self {
        Self::new(
            OpenAt::new(types::Fd(dir), path)
                .flags(flags)
                .mode(mode as mode_t)
                .build()
                .user_data(op.with_kind(OPEN).raw()),
        )
    }

    pub fn read(fd: RawFd, buf: &mut [u8], offset: u64, op: Token) -> Self {
        Self::read_raw(fd, buf.as_mut_ptr(), buf.len(), offset, op.with_kind(READ))
    }

    pub fn read_raw(fd: RawFd, ptr: *mut u8, len: usize, offset: u64, op: Token) -> Self {
        Self::new(
            Read::new(types::Fd(fd), ptr, len as u32)
                .offset(offset)
                .build()
                .user_data(op.raw()),
        )
    }

    pub fn stat_path(path: *const c_char, stat: *mut statx, op: Token) -> Self {
        Self::new(
            Statx::new(types::Fd(AT_FDCWD), path, stat.cast::<types::statx>())
                .mask(STATX_TYPE | STATX_SIZE | STATX_MTIME)
                .build()
                .user_data(op.with_kind(STAT).raw()),
        )
    }

    pub fn stat_fd(fd: RawFd, stat: *mut statx, op: Token) -> Self {
        Self::new(
            Statx::new(types::Fd(fd), c"".as_ptr(), stat.cast::<types::statx>())
                .flags(AT_EMPTY_PATH)
                .mask(STATX_TYPE | STATX_SIZE | STATX_MTIME)
                .build()
                .user_data(op.with_kind(STAT).raw()),
        )
    }

    pub fn recv_multi(fd: &Fd, buf_group: u16, op: Token) -> Self {
        Self::new(
            RecvMulti::new(Fixed(fd.slot().raw()), buf_group)
                .build()
                .user_data(op.with_kind(RECV).raw()),
        )
    }

    pub fn recv_discard(fd: &Fd, remaining: u64, op: Token) -> Self {
        const DISCARD_CAP: u64 = 1 << 30;
        static SCRATCH: u8 = 0;
        let len = remaining.min(DISCARD_CAP) as u32;
        Self::new(
            Recv::new(
                Fixed(fd.slot().raw()),
                &SCRATCH as *const u8 as *mut u8,
                len,
            )
            .flags(MSG_TRUNC)
            .build()
            .user_data(op.with_kind(RECV_DISCARD).raw()),
        )
    }

    pub fn accept_oneshot(
        listener: &Fd,
        addr_ptr: *mut sockaddr,
        addrlen_ptr: *mut socklen_t,
        op: Token,
    ) -> Self {
        Self::new(
            Accept::new(Fixed(listener.slot().raw()), addr_ptr, addrlen_ptr)
                .file_index(Some(DestinationSlot::auto_target()))
                .flags(0)
                .build()
                .user_data(op.with_kind(ACCEPT).raw()),
        )
    }

    pub fn recv_msg_multi(fd: &Fd, msghdr: &msghdr, buf_group: u16, op: Token) -> Self {
        Self::new(
            RecvMsgMulti::new(Fixed(fd.slot().raw()), msghdr, buf_group)
                .build()
                .user_data(op.with_kind(RECV).raw()),
        )
    }

    pub fn send_msg(fd: &Fd, msg: &msghdr, op: Token) -> Self {
        Self::new(
            SendMsg::new(Fixed(fd.slot().raw()), msg)
                .flags(MSG_NOSIGNAL as u32)
                .build()
                .user_data(op.with_kind(SEND).raw()),
        )
    }
}

impl Sqe {
    pub const SUPPORTS_RECV_DISCARD: bool = true;

    pub(crate) fn close_at(slot: FdSlot) -> Self {
        Self::new(
            Close::new(Fixed(slot.raw()))
                .build()
                .user_data(Self::framework(slot, CLOSE)),
        )
    }

    pub fn quickack(fd: &Fd) -> Self {
        const TCP_QUICKACK: u32 = 12;
        static QUICKACK_ON: c_int = 1;
        Self::new(
            SetSockOpt::new(
                Fixed(fd.slot().raw()),
                IPPROTO_TCP as u32,
                TCP_QUICKACK,
                &QUICKACK_ON as *const c_int as *const c_void,
                size_of::<c_int>() as u32,
            )
            .build()
            .user_data(0),
        )
    }

    pub(crate) fn shutdown_linked_at(slot: FdSlot, how: i32) -> Self {
        Self::new(
            Shutdown::new(Fixed(slot.raw()), how)
                .build()
                .flags(Flags::IO_HARDLINK)
                .user_data(Self::framework(slot, CLOSE_PREP)),
        )
    }

    pub(crate) fn poll_shutdown(fd: RawFd) -> Self {
        Self::new(
            PollAdd::new(types::Fd(fd), POLLIN as u32)
                .build()
                .user_data(SHUTDOWN.raw()),
        )
    }

    pub fn cancel(target: Token, op_kind: u8) -> Self {
        Self::new(
            AsyncCancel::new(target.with_kind(op_kind).raw())
                .build()
                .user_data(0),
        )
    }

    /// Arms a kernel-owned recurring timer.
    /// The multishot operation borrows `timer` until cancellation or driver
    /// teardown.
    pub fn interval(timer: &'static Timespec, op: Token) -> Self {
        Self::new(
            Timeout::new(timer)
                .count(0)
                .flags(TimeoutFlags::MULTISHOT)
                .build()
                .user_data(op.with_kind(TIMER).raw()),
        )
    }

    pub fn cancel_create(slot: FdSlot) -> Self {
        Self::new(
            AsyncCancel::new(Self::framework(slot, CREATE))
                .build()
                .user_data(0),
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

    pub(crate) fn socket_at(
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        op: Token,
    ) -> io::Result<Self> {
        let dest = DestinationSlot::try_from_slot_target(slot.raw())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "dope: socket slot out of range"))?;
        Ok(Self::create(
            Socket::new(domain, socket_type, protocol)
                .file_index(Some(dest))
                .build(),
            slot,
            op.with_kind(SOCKET),
        ))
    }
}

impl RawSqe {
    pub(crate) fn bind_at(
        slot: FdSlot,
        addr_ptr: *const sockaddr,
        addr_len: u32,
        op: Token,
    ) -> Self {
        Self::new(
            Bind::new(Fixed(slot.raw()), addr_ptr, addr_len)
                .build()
            .user_data(op.with_kind(SOCKET).raw()),
        )
    }
}

impl Sqe {
    pub(crate) fn listen_at(slot: FdSlot, backlog: i32, op: Token) -> Self {
        Self::new(
            Listen::new(Fixed(slot.raw()), backlog)
                .build()
                .user_data(op.with_kind(SOCKET).raw()),
        )
    }
}

impl RawSqe {
    pub fn connect(fd: &Fd, addr_ptr: *const sockaddr, addr_len: u32, op: Token) -> Self {
        Self::new(
            Connect::new(Fixed(fd.slot().raw()), addr_ptr, addr_len)
                .build()
                .user_data(op.with_kind(CONNECT).raw()),
        )
    }
}
