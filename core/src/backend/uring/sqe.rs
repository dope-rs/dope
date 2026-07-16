use std::io::{self, Error, ErrorKind};
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::slice;

use io_uring::types;
use o3::marker::ThreadBound;

use crate::driver::token::SHUTDOWN;
use crate::driver::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token, kind};
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
use io_uring::opcode::Splice;
use io_uring::opcode::Statx;
use io_uring::opcode::Timeout;
use io_uring::opcode::Write;
use io_uring::squeue::Entry;
use io_uring::squeue::Flags;
use io_uring::types::DestinationSlot;

#[derive(Clone, Copy)]
pub struct Create {
    pub slot: FdSlot,
    pub user_data: u64,
}

pub struct Sqe {
    entry: Entry,
    create: Option<Create>,
    _thread: ThreadBound,
}

impl Sqe {
    pub fn entry(&self) -> &Entry {
        &self.entry
    }

    pub fn create_meta(&self) -> Option<Create> {
        self.create
    }

    fn new(entry: Entry) -> Self {
        Self {
            entry,
            create: None,
            _thread: ThreadBound::NEW,
        }
    }

    fn create(entry: Entry, slot: FdSlot, user_data: u64) -> Self {
        let token = Token::new(ROUTE_FRAMEWORK, SlotIndex::new(slot.raw()), Epoch::ZERO)
            .with_kind(kind::CREATE);
        Self {
            entry: entry.user_data(token.raw()),
            create: Some(Create { slot, user_data }),
            _thread: ThreadBound::NEW,
        }
    }

    fn framework(slot: FdSlot, op_kind: u8) -> u64 {
        Token::new(ROUTE_FRAMEWORK, SlotIndex::new(slot.raw()), Epoch::ZERO)
            .with_kind(op_kind)
            .raw()
    }

    pub fn from_entry(entry: Entry) -> Self {
        Self::new(entry)
    }

    pub fn send(fd: &Fd, buf: &[u8], op: Token) -> Self {
        Self::send_at(fd.slot(), buf, op)
    }

    pub fn send_at(slot: FdSlot, buf: &[u8], op: Token) -> Self {
        Self::new(
            Send::new(types::Fixed(slot.raw()), buf.as_ptr(), buf.len() as u32)
                .flags(libc::MSG_NOSIGNAL)
                .build()
                .user_data(op.with_kind(kind::SEND).raw()),
        )
    }

    /// # Safety
    /// `fd` must stay open and `buf` stable and unchanged until completion.
    pub unsafe fn write_fd(fd: RawFd, buf: &[u8], offset: u64, op: Token) -> Self {
        Self::new(
            Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(op.with_kind(kind::WRITE).raw()),
        )
    }

    pub fn openat(dir: RawFd, path: *const libc::c_char, flags: i32, mode: u32, op: Token) -> Self {
        Self::new(
            OpenAt::new(types::Fd(dir), path)
                .flags(flags)
                .mode(mode as libc::mode_t)
                .build()
                .user_data(op.with_kind(kind::OPEN).raw()),
        )
    }

    /// # Safety
    /// `dir` and a NUL-terminated `path` must stay valid, and `slot` reserved, until completion.
    pub unsafe fn openat_fixed(
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        slot: FdSlot,
        op: Token,
    ) -> io::Result<Self> {
        let dest = DestinationSlot::try_from_slot_target(slot.raw())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "dope: open slot out of range"))?;
        Ok(Self::create(
            OpenAt::new(types::Fd(dir), path)
                .file_index(Some(dest))
                .flags(flags)
                .mode(mode as libc::mode_t)
                .build(),
            slot,
            op.with_kind(kind::OPEN).raw(),
        ))
    }

    /// # Safety
    /// `fd` must stay open and `buf` stable and unaliased until completion.
    pub unsafe fn read(fd: RawFd, buf: &mut [u8], offset: u64, op: Token) -> Self {
        let buf = unsafe {
            slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<MaybeUninit<u8>>(), buf.len())
        };
        unsafe { Self::read_uninit(fd, buf, offset, op.with_kind(kind::READ)) }
    }

    /// # Safety
    /// `fd` must stay open and `buf` stable and unaliased until completion.
    pub unsafe fn read_uninit(
        fd: RawFd,
        buf: &mut [MaybeUninit<u8>],
        offset: u64,
        op: Token,
    ) -> Self {
        Self::new(
            Read::new(types::Fd(fd), buf.as_mut_ptr().cast(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(op.raw()),
        )
    }

    pub fn read_fixed_file_uninit(
        slot: FdSlot,
        buf: &mut [MaybeUninit<u8>],
        offset: u64,
        op: Token,
    ) -> Self {
        Self::new(
            Read::new(
                types::Fixed(slot.raw()),
                buf.as_mut_ptr().cast(),
                buf.len() as u32,
            )
            .offset(offset)
            .build()
            .user_data(op.raw()),
        )
    }

    pub fn stat_path(path: *const libc::c_char, stat: *mut libc::statx, op: Token) -> Self {
        Self::new(
            Statx::new(types::Fd(libc::AT_FDCWD), path, stat.cast::<types::statx>())
                .mask(libc::STATX_TYPE | libc::STATX_SIZE | libc::STATX_MTIME)
                .build()
                .user_data(op.with_kind(kind::STAT).raw()),
        )
    }

    pub fn stat_fd(fd: RawFd, stat: *mut libc::statx, op: Token) -> Self {
        Self::new(
            Statx::new(types::Fd(fd), c"".as_ptr(), stat.cast::<types::statx>())
                .flags(libc::AT_EMPTY_PATH)
                .mask(libc::STATX_TYPE | libc::STATX_SIZE | libc::STATX_MTIME)
                .build()
                .user_data(op.with_kind(kind::STAT).raw()),
        )
    }

    /// # Safety
    /// Both descriptors must stay open until completion.
    pub unsafe fn splice_raw(
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
        flags: u32,
        op: Token,
    ) -> Self {
        Self::splice(fd_in, off_in, fd_out, off_out, len, flags, op)
    }

    fn splice(
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
        flags: u32,
        op: Token,
    ) -> Self {
        Self::new(
            Splice::new(types::Fd(fd_in), off_in, types::Fd(fd_out), off_out, len)
                .flags(flags)
                .build()
                .user_data(op.with_kind(kind::SPLICE).raw()),
        )
    }

    pub fn splice_to_pipe(
        fd_in: RawFd,
        off_in: i64,
        pipe_write_fd: RawFd,
        len: u32,
        op: Token,
    ) -> Self {
        Self::splice(fd_in, off_in, pipe_write_fd, -1, len, 0, op)
    }

    /// # Safety
    /// `fd` must belong to the receiving driver and stay live until completion.
    pub unsafe fn recv_multi(fd: &Fd, buf_group: u16, op: Token) -> Self {
        Self::new(
            RecvMulti::new(types::Fixed(fd.slot().raw()), buf_group)
                .build()
                .user_data(op.with_kind(kind::RECV).raw()),
        )
    }

    pub const SUPPORTS_RECV_DISCARD: bool = true;

    /// # Safety
    /// `fd` must belong to the receiving driver and stay live until completion.
    pub unsafe fn recv_discard(fd: &Fd, remaining: u64, op: Token) -> Self {
        const DISCARD_CAP: u64 = 1 << 30;
        static SCRATCH: u8 = 0;
        let len = remaining.min(DISCARD_CAP) as u32;
        Self::new(
            Recv::new(
                types::Fixed(fd.slot().raw()),
                &SCRATCH as *const u8 as *mut u8,
                len,
            )
            .flags(libc::MSG_TRUNC)
            .build()
            .user_data(op.with_kind(kind::RECV_DISCARD).raw()),
        )
    }

    pub fn accept_oneshot(
        listener: &Fd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        op: Token,
    ) -> Self {
        Self::new(
            Accept::new(types::Fixed(listener.slot().raw()), addr_ptr, addrlen_ptr)
                .file_index(Some(DestinationSlot::auto_target()))
                .flags(0)
                .build()
                .user_data(op.with_kind(kind::ACCEPT).raw()),
        )
    }

    pub fn recv_msg_multi(fd: &Fd, msghdr: &libc::msghdr, buf_group: u16, op: Token) -> Self {
        Self::new(
            RecvMsgMulti::new(types::Fixed(fd.slot().raw()), msghdr, buf_group)
                .build()
                .user_data(op.with_kind(kind::RECV).raw()),
        )
    }

    pub fn send_msg(fd: &Fd, msg: &libc::msghdr, op: Token) -> Self {
        Self::new(
            SendMsg::new(types::Fixed(fd.slot().raw()), msg)
                .flags(libc::MSG_NOSIGNAL as u32)
                .build()
                .user_data(op.with_kind(kind::SEND).raw()),
        )
    }

    pub fn close_at(slot: FdSlot) -> Self {
        Self::new(
            Close::new(types::Fixed(slot.raw()))
                .build()
                .user_data(Self::framework(slot, kind::CLOSE)),
        )
    }

    pub fn quickack(fd: &Fd) -> Self {
        const TCP_QUICKACK: u32 = 12;
        static QUICKACK_ON: libc::c_int = 1;
        Self::new(
            SetSockOpt::new(
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
        Self::new(
            Shutdown::new(types::Fixed(fd.slot().raw()), how)
                .build()
                .user_data(0),
        )
    }

    pub fn shutdown_linked_at(slot: FdSlot, how: i32) -> Self {
        Self::new(
            Shutdown::new(types::Fixed(slot.raw()), how)
                .build()
                .flags(Flags::IO_HARDLINK)
                .user_data(Self::framework(slot, kind::CLOSE_PREP)),
        )
    }

    pub fn poll_shutdown(fd: RawFd) -> Self {
        Self::new(
            PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
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
    ///
    /// The timer specification is referenced by the multishot operation and
    /// must therefore remain live until the driver is torn down or the timer
    /// is cancelled.
    pub fn interval(timer: &'static types::Timespec, op: Token) -> Self {
        Self::new(
            Timeout::new(timer)
                .count(0)
                .flags(types::TimeoutFlags::MULTISHOT)
                .build()
                .user_data(op.with_kind(kind::TIMER).raw()),
        )
    }

    pub fn cancel_create(slot: FdSlot) -> Self {
        Self::new(
            AsyncCancel::new(Self::framework(slot, kind::CREATE))
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

    pub fn socket_at(
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
            op.with_kind(kind::SOCKET).raw(),
        ))
    }

    pub fn bind_at(
        slot: FdSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        op: Token,
    ) -> Self {
        Self::new(
            Bind::new(types::Fixed(slot.raw()), addr_ptr, addr_len)
                .build()
                .user_data(op.with_kind(kind::SOCKET).raw()),
        )
    }

    pub fn listen_at(slot: FdSlot, backlog: i32, op: Token) -> Self {
        Self::new(
            Listen::new(types::Fixed(slot.raw()), backlog)
                .build()
                .user_data(op.with_kind(kind::SOCKET).raw()),
        )
    }

    pub fn connect(fd: &Fd, addr_ptr: *const libc::sockaddr, addr_len: u32, op: Token) -> Self {
        Self::new(
            Connect::new(types::Fixed(fd.slot().raw()), addr_ptr, addr_len)
                .build()
                .user_data(op.with_kind(kind::CONNECT).raw()),
        )
    }
}
