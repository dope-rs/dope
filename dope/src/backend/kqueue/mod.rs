mod config;
mod datagram;
mod errno;
pub mod platform;
pub(super) mod provided;
pub mod sqe;
mod system;
mod udata;

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::slice;
use std::time::Duration;

use crate::backend::{Drive, Lend, Sockopt};
use crate::backend::cqe::Cqe;
use std::net::SocketAddr;

use crate::backend::ListenerOpts;
use crate::backend::os_fd::OsFd;
use crate::backend::socket::{Addr, Domain, Fd, FdSlot, IoVec, Kind, MsgHdr};
use crate::backend::token::Token;

pub(super) use config::Config;

use self::errno::Errno;
use self::udata::Udata;

const TAG_ACCEPT: u8 = 1;
const TAG_RECV: u8 = 2;
const TAG_RECV_MSG: u8 = 3;
const TAG_WRITE_RETRY: u8 = 4;
const TAG_SHUTDOWN: u8 = 5;

const WAKE_IDENT: libc::uintptr_t = libc::uintptr_t::MAX;

const MAX_DRAIN_PER_FD: usize = 256;
const PENDING_CAP: usize = 1 << 16;
const CHANGES_FLUSH_AT: usize = 4096;
const SPLICE_BOUNCE: usize = 1 << 16;

#[derive(Clone, Copy, Debug, Default)]
pub struct Backend;

pub struct Driver {
    kq: OwnedFd,
    changes: Vec<libc::kevent>,
    accept_slots: HashMap<usize, AcceptSlot>,
    recv_slots: HashMap<usize, SlotHeader>,
    recvmsg_slots: HashMap<usize, RecvMsgSlot>,
    write_retries: Vec<Option<WriteRetry>>,
    write_retry_free: Vec<u32>,
    write_retry_fd: HashMap<RawFd, u32>,
    resume: VecDeque<Resume>,
    pending: VecDeque<PendingCompletion>,
    provided: provided::Pool,
    fd_table: Vec<Option<RawFd>>,
    arena: Box<crate::backend::park::Arena>,
    accept_limit: u32,
    next_slot: u32,
    alive: &'static Cell<bool>,
}

#[derive(Clone, Copy)]
struct WriteRetry {
    ud: Token,
    fd: RawFd,
    epoch: u32,
    kind: WriteKind,
}

#[derive(Clone, Copy)]
enum WriteKind {
    Send { ptr: *const u8, len: u32 },
    SendMsg { msg: *const libc::msghdr },
    Connect {
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
    },
}

struct SlotHeader {
    fd: RawFd,
    epoch: u32,
    armed: bool,
    ud: Token,
}

impl SlotHeader {
    fn validate(
        &mut self,
        ev: &libc::kevent,
        epoch: u32,
    ) -> Result<(RawFd, Token), Option<(Token, i32)>> {
        if self.epoch != epoch || !self.armed {
            return Err(None);
        }
        if (ev.flags & libc::EV_ERROR) != 0 && ev.data != 0 {
            self.armed = false;
            return Err(Some((self.ud, ev.data as i32)));
        }
        Ok((self.fd, self.ud))
    }
}

struct AcceptSlot {
    hdr: SlotHeader,
    addr_ptr: *mut libc::sockaddr,
    addrlen_ptr: *mut libc::socklen_t,
    oneshot: bool,
}

struct RecvMsgSlot {
    hdr: SlotHeader,
    msg_template: *const libc::msghdr,
}

#[derive(Clone, Copy)]
enum Resume {
    Accept(usize),
    Recv(usize),
    RecvMsg(usize),
}

#[derive(Clone, Copy, Debug)]
enum PendingCompletion {
    Accept {
        ud: Token,
        result: i32,
        more: bool,
    },
    Recv {
        ud: Token,
        result: i32,
        more: bool,
        bid: Option<u16>,
    },
    Write {
        ud: Token,
        result: i32,
    },
    Timer {
        ud: Token,
    },
    Shutdown,
}

enum DrainOutcome {
    Done,
    Yield,
    Closed,
}

impl Drop for Driver {
    fn drop(&mut self) {
        // Mark dead before the fd table drops so out-of-order `Fd`s skip close.
        self.alive.set(false);
    }
}

impl Driver {
    pub fn open(cfg: Config) -> io::Result<Self> {
        Self::new(cfg)
    }

    pub fn new(cfg: Config) -> io::Result<Self> {
        <Backend as crate::backend::Backend>::init_process(&cfg)?;
        let _ = system::Snapshot::raise_nofile();
        // SAFETY: kqueue(2) returns a valid fd or -1; checked below.
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: kqueue returned a fresh descriptor and ownership is transferred here.
        let kq = unsafe { OwnedFd::from_raw_fd(raw) };
        let rc = unsafe { libc::fcntl(kq.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        let wake = libc::kevent {
            ident: WAKE_IDENT,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: kq is a live kqueue fd; `wake` is one valid kevent registering an EVFILT_USER wake source.
        let rc = unsafe {
            libc::kevent(kq.as_raw_fd(), &wake, 1, std::ptr::null_mut(), 0, std::ptr::null())
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        let fixed_file_slots = cfg.fixed_file_slots.max(cfg.accept_slots).max(1);
        let slots = fixed_file_slots as usize;
        Ok(Self {
            kq,
            changes: Vec::with_capacity(64),
            accept_slots: HashMap::new(),
            recv_slots: HashMap::new(),
            recvmsg_slots: HashMap::new(),
            write_retries: Vec::new(),
            write_retry_free: Vec::new(),
            write_retry_fd: HashMap::new(),
            resume: VecDeque::new(),
            pending: VecDeque::new(),
            provided: provided::Pool::new(cfg.provided_buf_entries, cfg.provided_buf_len as u32),
            fd_table: vec![None; slots],
            arena: crate::backend::park::Arena::new(slots)?,
            accept_limit: cfg.accept_slots.min(fixed_file_slots),
            next_slot: fixed_file_slots,
            alive: Box::leak(Box::new(Cell::new(true))),
        })
    }

    pub(crate) fn alive_handle(&self) -> &'static Cell<bool> {
        self.alive
    }

    pub(super) fn alloc_fixed_range(&mut self, len: u32) -> io::Result<u32> {
        let base = self
            .next_slot
            .checked_sub(len)
            .filter(|&b| b >= self.accept_limit)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::OutOfMemory, "dope: fixed-file slots exhausted")
            })?;
        self.next_slot = base;
        Ok(base)
    }

    pub(super) fn register_raw_fd(&mut self, slot: u32, raw: RawFd) -> io::Result<()> {
        let idx = slot as usize;
        if idx >= self.fd_table.len() {
            self.fd_table.resize(idx + 1, None);
        }
        let cell = &mut self.fd_table[idx];
        if let Some(old) = cell.replace(raw) {
            self.close_raw(old);
        }
        Ok(())
    }

    fn raw_fd(&self, slot: FdSlot) -> Option<RawFd> {
        self.fd_table.get(slot.raw() as usize).and_then(|v| *v)
    }

    fn close_raw(&mut self, raw: RawFd) {
        self.write_retry_fd.remove(&raw);
        // SAFETY: raw is owned by the kqueue fd table and is consumed here.
        drop(OsFd::take(raw));
    }

    fn release_slot(&mut self, slot: FdSlot) {
        let idx = slot.raw() as usize;
        if let Some(raw) = self.fd_table.get_mut(idx).and_then(Option::take) {
            self.close_raw(raw);
        }
    }

    pub(super) fn adopt_fd_raw(&mut self, idx: u32) -> Fd {
        Fd::adopt(FdSlot::new(idx), self)
    }

    pub fn reserve_outbound(&mut self, count: u32) -> io::Result<crate::backend::OutboundReservation> {
        let base = self.alloc_fixed_range(count)?;
        Ok(crate::backend::OutboundReservation::new(base, count))
    }

    pub(super) fn release_fd_slot(&mut self, slot: FdSlot) {
        self.release_slot(slot);
    }

    fn next_accept_slot(&self) -> Option<u32> {
        self.fd_table
            .iter()
            .take(self.accept_limit as usize)
            .position(Option::is_none)
            .map(|idx| idx as u32)
    }

    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.resume_pending();
        let mut events: [MaybeUninit<libc::kevent>; 64] = [const { MaybeUninit::uninit() }; 64];
        let n = self.kevent_call(&mut events, timeout)?;
        // SAFETY: kevent(2) wrote `n` valid entries into `events`; MaybeUninit<kevent> has the same layout as kevent.
        let ready =
            unsafe { slice::from_raw_parts(events.as_ptr().cast::<libc::kevent>(), n) };
        for ev in ready {
            self.dispatch_event(ev);
        }
        Ok(n)
    }

    fn kevent_call(
        &mut self,
        events: &mut [MaybeUninit<libc::kevent>],
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let ts_storage;
        let ts_ptr: *const libc::timespec = match timeout {
            None => std::ptr::null(),
            Some(d) => {
                ts_storage = libc::timespec {
                    tv_sec: d.as_secs() as libc::time_t,
                    tv_nsec: d.subsec_nanos() as libc::c_long,
                };
                &ts_storage
            }
        };
        // SAFETY: kq is a valid kqueue fd; changes/events are valid slices for the duration of the call.
        let n = unsafe {
            libc::kevent(
                self.kq.as_raw_fd(),
                self.changes.as_ptr(),
                self.changes.len() as libc::c_int,
                events.as_mut_ptr().cast(),
                events.len() as libc::c_int,
                ts_ptr,
            )
        };
        self.changes.clear();
        if n < 0 {
            let err = io::Error::last_os_error();
            return if err.raw_os_error() == Some(libc::EINTR) {
                Ok(0)
            } else {
                Err(err)
            };
        }
        Ok(n as usize)
    }

    fn flush_changes_if_full(&mut self) {
        if self.changes.len() >= CHANGES_FLUSH_AT {
            let _ = self.kevent_call(&mut [], Some(Duration::ZERO));
        }
    }
}

impl Driver {
    fn dispatch_event(&mut self, ev: &libc::kevent) {
        if ev.filter == libc::EVFILT_USER {
            return;
        }
        if ev.filter == libc::EVFILT_TIMER {
            if let Some(ud) = Token::try_from_raw(ev.udata as usize as u64) {
                self.push_pending(PendingCompletion::Timer { ud });
            }
            return;
        }
        let raw = Udata::from_kevent(ev.udata);
        let (tag, route, slot, epoch) = raw.unpack();
        match tag {
            TAG_ACCEPT => self.dispatch_accept(Udata::slot_key(route, slot), epoch, ev),
            TAG_RECV => self.dispatch_recv(Udata::slot_key(route, slot), epoch, ev),
            TAG_RECV_MSG => self.dispatch_recv_msg(Udata::slot_key(route, slot), epoch, ev),
            TAG_WRITE_RETRY => self.dispatch_write_retry(slot, epoch),
            TAG_SHUTDOWN => self.push_pending(PendingCompletion::Shutdown),
            _ => {}
        }
    }

    fn dispatch_accept(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent) {
        let outcome = {
            let Some(slot) = self.accept_slots.get_mut(&slot_idx) else { return };
            match slot.hdr.validate(ev, epoch) {
                Ok((fd, ud)) => Ok((fd, ud, slot.addr_ptr, slot.addrlen_ptr, slot.oneshot)),
                Err(e) => Err(e),
            }
        };
        let (fd, ud, addr_ptr, addrlen_ptr, oneshot) = match outcome {
            Ok(t) => t,
            Err(None) => return,
            Err(Some((ud, data))) => {
                self.push_pending(PendingCompletion::Accept {
                    ud,
                    result: -data,
                    more: false,
                });
                return;
            }
        };
        match self.drain_accept(fd, ud, addr_ptr, addrlen_ptr, oneshot) {
            DrainOutcome::Done => {
                if !oneshot {
                    self.re_enable_read(fd, Udata::from_token(ud, TAG_ACCEPT));
                } else if let Some(s) = self.accept_slots.get_mut(&slot_idx) {
                    s.hdr.armed = false;
                }
            }
            DrainOutcome::Yield => self.resume.push_back(Resume::Accept(slot_idx)),
            DrainOutcome::Closed => {}
        }
    }

    fn drain_accept(
        &mut self,
        fd: RawFd,
        ud: Token,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
        oneshot: bool,
    ) -> DrainOutcome {
        for _ in 0..MAX_DRAIN_PER_FD {
            if self.pending.len() >= PENDING_CAP {
                return DrainOutcome::Yield;
            }
            if !addrlen_ptr.is_null() {
                // SAFETY: addrlen_ptr is non-null and points to a writable socklen_t (Accept buffer lives across re-arm).
                unsafe {
                    *addrlen_ptr = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                }
            }
            // SAFETY: `fd` is a live listening socket for the duration of drain_accept; addr_ptr/addrlen_ptr are either null or valid.
            let raw = unsafe { libc::accept(fd, addr_ptr, addrlen_ptr) };
            let more_flag = !oneshot;
            if raw >= 0 {
                let Some(slot) = self.next_accept_slot() else {
                    self.close_raw(raw);
                    self.push_pending(PendingCompletion::Accept {
                        ud,
                        result: -libc::EMFILE,
                        more: more_flag,
                    });
                    return DrainOutcome::Done;
                };
                let accepted = OsFd::take(raw);
                if accepted.set_nonblocking().is_err() || accepted.set_cloexec().is_err() {
                    continue;
                }
                let raw = accepted.into_raw_fd();
                if self.register_raw_fd(slot, raw).is_err() {
                    self.close_raw(raw);
                    self.push_pending(PendingCompletion::Accept {
                        ud,
                        result: -libc::EMFILE,
                        more: more_flag,
                    });
                    return DrainOutcome::Done;
                }
                self.push_pending(PendingCompletion::Accept {
                    ud,
                    result: slot as i32,
                    more: more_flag,
                });
                if oneshot {
                    return DrainOutcome::Done;
                }
                continue;
            }
            let errno = Errno::last_raw();
            if Errno::is_block_raw(errno) {
                return DrainOutcome::Done;
            }
            self.push_pending(PendingCompletion::Accept {
                ud,
                result: -errno,
                more: more_flag,
            });
            return DrainOutcome::Done;
        }
        DrainOutcome::Yield
    }

    fn dispatch_recv(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent) {
        let outcome = match self.recv_slots.get_mut(&slot_idx) {
            Some(h) => h.validate(ev, epoch),
            None => return,
        };
        let (fd, ud) = match outcome {
            Ok(p) => p,
            Err(None) => return,
            Err(Some((ud, data))) => {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: -data,
                    more: false,
                    bid: None,
                });
                return;
            }
        };
        match self.drain_recv(fd, ud) {
            DrainOutcome::Done => self.re_enable_read(fd, Udata::from_token(ud, TAG_RECV)),
            DrainOutcome::Yield => self.resume.push_back(Resume::Recv(slot_idx)),
            DrainOutcome::Closed => {}
        }
    }

    fn drain_recv(&mut self, fd: RawFd, ud: Token) -> DrainOutcome {
        for _ in 0..MAX_DRAIN_PER_FD {
            if self.pending.len() >= PENDING_CAP {
                return DrainOutcome::Yield;
            }
            let Some(bid) = self.provided.pop_free() else {
                return DrainOutcome::Yield;
            };
            let (ptr, cap) = self.provided.ptr_len(bid);
            // SAFETY: `ptr`/`cap` are from `provided.ptr_len(bid)`, a live buffer for this bid.
            let n = unsafe { libc::recv(fd, ptr.cast(), cap, 0) };
            if n > 0 {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: n as i32,
                    more: true,
                    bid: Some(bid),
                });
                continue;
            }
            if n == 0 {
                self.provided.defer(bid);
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: 0,
                    more: false,
                    bid: None,
                });
                return DrainOutcome::Closed;
            }
            let errno = Errno::last_raw();
            self.provided.defer(bid);
            if Errno::is_block_raw(errno) {
                return DrainOutcome::Done;
            }
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -errno,
                more: true,
                bid: None,
            });
            return DrainOutcome::Done;
        }
        DrainOutcome::Yield
    }

    fn dispatch_recv_msg(&mut self, slot_idx: usize, epoch: u32, ev: &libc::kevent) {
        let outcome = {
            let Some(slot) = self.recvmsg_slots.get_mut(&slot_idx) else {
                return;
            };
            match slot.hdr.validate(ev, epoch) {
                Ok((fd, ud)) => Ok((fd, ud, slot.msg_template)),
                Err(e) => Err(e),
            }
        };
        let (fd, ud, msg_tpl) = match outcome {
            Ok(t) => t,
            Err(None) => return,
            Err(Some((ud, data))) => {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: -data,
                    more: false,
                    bid: None,
                });
                return;
            }
        };
        match self.drain_recv_msg(fd, ud, msg_tpl) {
            DrainOutcome::Done => {
                self.re_enable_read(fd, Udata::from_token(ud, TAG_RECV_MSG))
            }
            DrainOutcome::Yield => self.resume.push_back(Resume::RecvMsg(slot_idx)),
            DrainOutcome::Closed => {}
        }
    }

    fn drain_recv_msg(
        &mut self,
        fd: RawFd,
        ud: Token,
        msg_tpl: *const libc::msghdr,
    ) -> DrainOutcome {
        // SAFETY: `msg_tpl` points to a `MsgHdr` pinned for the lifetime of the `RecvMsgSlot`.
        let template = unsafe { *msg_tpl };
        let namelen = template.msg_namelen as usize;
        for _ in 0..MAX_DRAIN_PER_FD {
            if self.pending.len() >= PENDING_CAP {
                return DrainOutcome::Yield;
            }
            let Some(bid) = self.provided.pop_free() else {
                return DrainOutcome::Yield;
            };
            let (ptr, cap) = self.provided.ptr_len(bid);
            if cap <= namelen {
                self.provided.defer(bid);
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: -libc::ENOBUFS,
                    more: true,
                    bid: None,
                });
                return DrainOutcome::Done;
            }
            // SAFETY: `ptr`/`cap` from `provided.ptr_len(bid)`; `namelen < cap` checked above.
            let iov = IoVec::from_mut_slice(unsafe {
                slice::from_raw_parts_mut(ptr.add(namelen), cap - namelen)
            });
            let mut local_msg = MsgHdr::empty();
            local_msg.set_name_ptr(ptr.cast(), template.msg_namelen);
            local_msg.set_iov(slice::from_ref(&iov));
            // SAFETY: `fd` is a live socket; `local_msg` is a fully-initialized msghdr for this call.
            let n = unsafe { libc::recvmsg(fd, local_msg.as_mut_ptr(), 0) };
            if n > 0 {
                if local_msg.flags() & libc::MSG_TRUNC != 0 {
                    self.provided.defer(bid);
                    continue;
                }
                let total = namelen + n as usize;
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: total as i32,
                    more: true,
                    bid: Some(bid),
                });
                continue;
            }
            if n == 0 {
                self.push_pending(PendingCompletion::Recv {
                    ud,
                    result: namelen as i32,
                    more: true,
                    bid: Some(bid),
                });
                return DrainOutcome::Done;
            }
            let errno = Errno::last_raw();
            self.provided.defer(bid);
            if Errno::is_block_raw(errno) {
                return DrainOutcome::Done;
            }
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -errno,
                more: true,
                bid: None,
            });
            return DrainOutcome::Done;
        }
        DrainOutcome::Yield
    }

    fn dispatch_write_retry(&mut self, idx: u32, epoch: u32) {
        let Some(retry) = self.take_write_retry(idx, epoch) else {
            return;
        };
        self.write_retry_fd.remove(&retry.fd);
        // SAFETY: `retry.fd` is a live socket; `ptr`/`msg` are valid because the caller kept them alive until this completion.
        let result: i32 = match retry.kind {
            WriteKind::Send { ptr, len } => {
                let rc = unsafe { libc::send(retry.fd, ptr.cast(), len as usize, 0) };
                if rc == -1 { -Errno::last_raw() } else { rc as i32 }
            }
            WriteKind::SendMsg { msg } => {
                let rc = unsafe { libc::sendmsg(retry.fd, msg, 0) };
                if rc == -1 { -Errno::last_raw() } else { rc as i32 }
            }
            WriteKind::Connect { addr_ptr, addr_len } => {
                let mut err = 0 as libc::c_int;
                let mut len = size_of::<libc::c_int>() as libc::socklen_t;
                // SAFETY: retry.fd is live; err/len are valid output buffers.
                let rc = unsafe {
                    libc::getsockopt(
                        retry.fd,
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        (&mut err as *mut libc::c_int).cast(),
                        &mut len,
                    )
                };
                if rc == 0 && (err == libc::EINPROGRESS || err == libc::EALREADY) {
                    // SAFETY: retry.fd is live; addr_ptr/addr_len remain pinned by the connecting slot.
                    let rc = unsafe {
                        libc::connect(retry.fd, addr_ptr, addr_len as libc::socklen_t)
                    };
                    if rc == 0 {
                        0
                    } else {
                        let errno = Errno::last_raw();
                        if errno == libc::EINPROGRESS || errno == libc::EALREADY {
                            let _ = self.arm_write_retry(
                                retry.fd,
                                retry.ud,
                                WriteKind::Connect { addr_ptr, addr_len },
                            );
                            return;
                        }
                        if errno == libc::EISCONN { 0 } else { -errno }
                    }
                } else if rc == 0 && err == 0 {
                    0
                } else if rc == 0 {
                    -err
                } else {
                    -Errno::last_raw()
                }
            }
        };
        self.push_pending(PendingCompletion::Write {
            ud: retry.ud,
            result,
        });
    }

    fn resume_pending(&mut self) {
        let n = self.resume.len();
        for _ in 0..n {
            let Some(item) = self.resume.pop_front() else {
                break;
            };
            match item {
                Resume::Accept(slot_idx) => {
                    let Some((fd, ud, _epoch, addr_ptr, addrlen_ptr, oneshot)) = self
                        .accept_slots
                        .get(&slot_idx)
                        .filter(|s| s.hdr.armed)
                        .map(|s| {
                            (s.hdr.fd, s.hdr.ud, s.hdr.epoch, s.addr_ptr, s.addrlen_ptr, s.oneshot)
                        })
                    else {
                        continue;
                    };
                    match self.drain_accept(fd, ud, addr_ptr, addrlen_ptr, oneshot) {
                        DrainOutcome::Done => {
                            if !oneshot {
                                self.re_enable_read(fd, Udata::from_token(ud, TAG_ACCEPT))
                            } else if let Some(s) = self.accept_slots.get_mut(&slot_idx) {
                                s.hdr.armed = false;
                            }
                        }
                        DrainOutcome::Yield => self.resume.push_back(Resume::Accept(slot_idx)),
                        DrainOutcome::Closed => {}
                    }
                }
                Resume::Recv(slot_idx) => {
                    let Some((fd, ud, _epoch)) = self
                        .recv_slots
                        .get(&slot_idx)
                        .filter(|h| h.armed)
                        .map(|h| (h.fd, h.ud, h.epoch))
                    else {
                        continue;
                    };
                    match self.drain_recv(fd, ud) {
                        DrainOutcome::Done => {
                            self.re_enable_read(fd, Udata::from_token(ud, TAG_RECV))
                        }
                        DrainOutcome::Yield => self.resume.push_back(Resume::Recv(slot_idx)),
                        DrainOutcome::Closed => {}
                    }
                }
                Resume::RecvMsg(slot_idx) => {
                    let Some((fd, ud, _epoch, tpl)) = self
                        .recvmsg_slots
                        .get(&slot_idx)
                        .filter(|s| s.hdr.armed)
                        .map(|s| (s.hdr.fd, s.hdr.ud, s.hdr.epoch, s.msg_template))
                    else {
                        continue;
                    };
                    match self.drain_recv_msg(fd, ud, tpl) {
                        DrainOutcome::Done => self
                            .re_enable_read(fd, Udata::from_token(ud, TAG_RECV_MSG)),
                        DrainOutcome::Yield => self.resume.push_back(Resume::RecvMsg(slot_idx)),
                        DrainOutcome::Closed => {}
                    }
                }
            }
        }
    }
}

impl Driver {
    fn push_pending(&mut self, c: PendingCompletion) {
        if self.pending.is_empty() {
            self.changes.push(libc::kevent {
                ident: WAKE_IDENT,
                filter: libc::EVFILT_USER,
                flags: libc::EV_ENABLE,
                fflags: libc::NOTE_TRIGGER,
                data: 0,
                udata: std::ptr::null_mut(),
            });
        }
        self.pending.push_back(c);
    }

    fn set_fd_nonblocking(raw: RawFd) -> io::Result<()> {
        // SAFETY: `raw` is a live socket fd owned by the conn layer for the duration of this arm.
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL, 0) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` is a live socket fd owned by the conn layer for the duration of this arm.
        let rc = unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn arm_read_multi(&mut self, raw: RawFd, udata: Udata) -> io::Result<()> {
        Self::set_fd_nonblocking(raw)?;
        self.changes.push(libc::kevent {
            ident: raw as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_DISPATCH,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        });
        self.flush_changes_if_full();
        Ok(())
    }

    fn re_enable_read(&mut self, raw: RawFd, udata: Udata) {
        self.changes.push(libc::kevent {
            ident: raw as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ENABLE | libc::EV_DISPATCH,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        });
        self.flush_changes_if_full();
    }

    fn disarm_filter(&mut self, fd: RawFd, filter: i16) {
        if filter == 0 {
            return;
        }
        self.changes.push(libc::kevent {
            ident: fd as libc::uintptr_t,
            filter,
            flags: libc::EV_DELETE,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        });
        self.flush_changes_if_full();
    }

    fn alloc_write_retry(&mut self, mut retry: WriteRetry) -> (u32, u32) {
        if let Some(idx) = self.write_retry_free.pop() {
            let prev_epoch = self.write_retries[idx as usize]
                .as_ref()
                .map(|r| r.epoch)
                .unwrap_or(0);
            let epoch = prev_epoch.wrapping_add(1).max(1);
            retry.epoch = epoch;
            self.write_retries[idx as usize] = Some(retry);
            (idx, epoch)
        } else {
            let idx = u32::try_from(self.write_retries.len())
                .expect("dope-kqueue: write_retries index overflow");
            retry.epoch = 1;
            self.write_retries.push(Some(retry));
            (idx, 1)
        }
    }

    fn take_write_retry(&mut self, idx: u32, epoch: u32) -> Option<WriteRetry> {
        let slot = self.write_retries.get_mut(idx as usize)?;
        match slot {
            Some(r) if r.epoch == epoch => {
                let r = slot.take();
                self.write_retry_free.push(idx);
                r
            }
            _ => None,
        }
    }

    fn arm_write_retry(&mut self, fd: RawFd, ud: Token, kind: WriteKind) -> bool {
        if self.write_retry_fd.contains_key(&fd) {
            return false;
        }
        let retry = WriteRetry {
            ud,
            fd,
            epoch: 0,
            kind,
        };
        let (idx, epoch) = self.alloc_write_retry(retry);
        self.write_retry_fd.insert(fd, idx);
        let udata = Udata::pack(TAG_WRITE_RETRY, idx, epoch);
        self.changes.push(libc::kevent {
            ident: fd as libc::uintptr_t,
            filter: libc::EVFILT_WRITE,
            flags: libc::EV_ADD | libc::EV_CLEAR | libc::EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: udata.into_kevent(),
        });
        self.flush_changes_if_full();
        true
    }

    fn dispatch_push(&mut self, sqe: sqe::Sqe) -> bool {
        match sqe.0 {
            sqe::SqeInner::AcceptOneshot { listener, addr_ptr, addrlen_ptr, ud } => {
                let ud = Token::from_raw(ud);
                let Some(raw) = self.raw_fd(listener) else {
                    self.push_pending(PendingCompletion::Accept {
                        ud,
                        result: -libc::EBADF,
                        more: false,
                    });
                    return true;
                };
                self.arm_accept_oneshot_inner(ud, raw, addr_ptr, addrlen_ptr)
            }
            sqe::SqeInner::RecvMulti { slot, ud } => {
                self.arm_recv_multi_inner(Token::from_raw(ud), slot)
            }
            // SAFETY: `msghdr` pointer validity is the caller's contract (sqe was constructed with `unsafe fn`).
            sqe::SqeInner::RecvMsgMulti { slot, msghdr, ud } => unsafe {
                self.arm_recv_msg_multi_inner(Token::from_raw(ud), slot, msghdr)
            },
            sqe::SqeInner::Send { slot, ptr, len, ud } => {
                self.submit_send_tagged_inner(Token::from_raw(ud), slot, ptr, len)
            }
            sqe::SqeInner::WriteFd { fd, ptr, len, offset, ud } => {
                self.submit_write_fd_inner(Token::from_raw(ud), fd, ptr, len, offset)
            }
            sqe::SqeInner::Fsync { fd, ud } => {
                self.submit_fsync_inner(Token::from_raw(ud), fd)
            }
            sqe::SqeInner::OpenAt { dir, path, flags, mode, ud } => {
                self.submit_openat_inner(Token::from_raw(ud), dir, path, flags, mode, None)
            }
            sqe::SqeInner::OpenAtFixed { dir, path, flags, mode, slot, ud } => {
                self.submit_openat_inner(Token::from_raw(ud), dir, path, flags, mode, Some(slot))
            }
            sqe::SqeInner::Read { fd, ptr, len, offset, ud } => {
                self.submit_read_inner(Token::from_raw(ud), fd, ptr, len, offset)
            }
            sqe::SqeInner::ReadFixed { slot, ptr, len, offset, ud } => match self.raw_fd(slot) {
                Some(fd) => self.submit_read_inner(Token::from_raw(ud), fd, ptr, len, offset),
                None => {
                    self.push_pending(PendingCompletion::Write { ud: Token::from_raw(ud), result: -libc::EBADF });
                    true
                }
            },
            sqe::SqeInner::Splice { fd_in, off_in, fd_out, off_out, len, ud } => {
                self.submit_splice_inner(Token::from_raw(ud), fd_in, off_in, fd_out, off_out, len)
            }
            // SAFETY: `msg` pointer validity is the caller's contract (sqe was constructed with `unsafe fn`).
            sqe::SqeInner::SendMsg { slot, msg, ud } => unsafe {
                self.submit_send_msg_tagged_inner(Token::from_raw(ud), slot, msg)
            },
            sqe::SqeInner::Close { slot } => {
                self.release_slot(slot);
                true
            }
            sqe::SqeInner::Quickack => true,
            sqe::SqeInner::Shutdown { slot, how } => {
                if let Some(raw) = self.raw_fd(slot) {
                    // SAFETY: `raw` is a connected socket fd; `how` is a valid SHUT_* constant.
                    unsafe { libc::shutdown(raw, how) };
                }
                true
            }
            sqe::SqeInner::PollShutdown { fd } => {
                self.arm_read_multi(fd, Udata::pack(TAG_SHUTDOWN, 0, 0)).is_ok()
            }
            sqe::SqeInner::Cancel { target } => {
                if let Some(t) = Token::try_from_raw(target) {
                    self.cancel_recv_inner(t);
                }
                true
            }
            sqe::SqeInner::Interval { sec, nsec, ud } => {
                let us = (sec as i128 * 1_000_000 + nsec as i128 / 1_000)
                    .clamp(0, libc::intptr_t::MAX as i128) as libc::intptr_t;
                self.changes.push(libc::kevent {
                    ident: ud as libc::uintptr_t,
                    filter: libc::EVFILT_TIMER,
                    flags: libc::EV_ADD,
                    fflags: libc::NOTE_USECONDS,
                    data: us,
                    udata: ud as usize as *mut libc::c_void,
                });
                self.flush_changes_if_full();
                true
            }
            sqe::SqeInner::SocketAt { domain, socket_type, protocol, slot, ud } => {
                self.submit_socket_at(domain, socket_type, protocol, slot, Token::from_raw(ud))
            }
            sqe::SqeInner::Connect { slot, addr_ptr, addr_len, ud } => {
                self.submit_connect(slot, addr_ptr, addr_len, Token::from_raw(ud))
            }
        }
    }
}

impl crate::backend::park::Parker for Driver {
    fn slot(&self, slot: FdSlot) -> &crate::backend::park::Slot {
        self.arena.slot(slot)
    }

    fn make_slot(&self, target: Token) -> crate::backend::park::Slot {
        crate::backend::park::Slot::new(target, std::ptr::NonNull::from(&*self.arena))
    }

    fn drain(&self, out: &mut Vec<Token>) {
        self.arena.drain(out);
    }

    fn is_empty(&self) -> bool {
        self.arena.is_empty()
    }
}

impl Drive for Driver {
    type Sqe = sqe::Sqe;

    fn push(&mut self, sqe: sqe::Sqe) -> Result<(), crate::backend::PushError> {
        if self.dispatch_push(sqe) {
            Ok(())
        } else {
            Err(crate::backend::PushError)
        }
    }

    fn submit_to_drain(&mut self) -> bool {
        false
    }

    fn drain(&mut self, buf: &mut [Cqe]) -> usize {
        use crate::backend::cqe;
        if self.pending.is_empty() {
            let _ = self.poll(Some(Duration::ZERO));
        }
        let mut n = 0;
        while n < buf.len() {
            let Some(p) = self.pending.pop_front() else {
                break;
            };
            buf[n] = match p {
                PendingCompletion::Accept { ud, result, more } => Cqe {
                    user_data: ud.raw(),
                    result,
                    flags: if more { cqe::MORE } else { 0 },
                },
                PendingCompletion::Recv {
                    ud,
                    result,
                    more,
                    bid,
                } => {
                    let mut f = if more { cqe::MORE } else { 0 };
                    if let Some(b) = bid {
                        f |= cqe::BUFFER | ((b as u32) << cqe::BUFFER_SHIFT);
                    }
                    Cqe {
                        user_data: ud.raw(),
                        result,
                        flags: f,
                    }
                }
                PendingCompletion::Write { ud, result } => Cqe {
                    user_data: ud.raw(),
                    result,
                    flags: 0,
                },
                PendingCompletion::Timer { ud } => Cqe {
                    user_data: ud.raw(),
                    result: 0,
                    flags: 0,
                },
                PendingCompletion::Shutdown => Cqe {
                    user_data: crate::backend::token::SHUTDOWN.raw(),
                    result: 0,
                    flags: 0,
                },
            };
            n += 1;
        }
        n
    }

    fn park(&mut self, timeout: Duration) -> io::Result<()> {
        self.poll(Some(timeout)).map(|_| ())
    }
}

impl Sockopt for Driver {
    fn set(
        &mut self,
        fixed_idx: u32,
        level: u32,
        optname: u32,
        value: i32,
    ) -> Result<(), crate::backend::PushError> {
        let Some(raw) = self.raw_fd(FdSlot::new(fixed_idx)) else {
            return Err(crate::backend::PushError);
        };
        // SAFETY: raw is a live socket; value points to a live c_int of the specified size.
        let rc = unsafe {
            libc::setsockopt(
                raw,
                level as libc::c_int,
                optname as libc::c_int,
                (&value as *const libc::c_int).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc == 0 { Ok(()) } else { Err(crate::backend::PushError) }
    }
}

impl Lend for Driver {
    fn group(&self) -> u16 {
        0
    }

    fn release(&mut self, bid: Option<u16>) {
        let Some(b) = bid else { return };
        self.provided.defer(b);
        if !self.resume.is_empty() {
            self.resume_pending();
        }
    }

    unsafe fn slice<'a>(&self, len: u32, bid: u16) -> &'a [u8] {
        let (ptr, _) = self.provided.ptr_len(bid);
        // SAFETY: caller guarantees `bid` is valid + held until `release`; len is byte count <= buffer cap.
        unsafe { slice::from_raw_parts(ptr, len as usize) }
    }
}

impl Driver {
    fn arm_accept_oneshot_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        addr_ptr: *mut libc::sockaddr,
        addrlen_ptr: *mut libc::socklen_t,
    ) -> bool {
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        let epoch = ud.epoch().raw();
        self.accept_slots.insert(
            slot_idx,
            AcceptSlot {
                hdr: SlotHeader { fd, epoch, armed: true, ud },
                addr_ptr,
                addrlen_ptr,
                oneshot: true,
            },
        );
        self.arm_read_multi(fd, Udata::from_token(ud, TAG_ACCEPT)).is_ok()
    }

    fn arm_recv_multi_inner(&mut self, ud: Token, slot: FdSlot) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -libc::EBADF,
                more: false,
                bid: None,
            });
            return true;
        };
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        let epoch = ud.epoch().raw();
        self.recv_slots.insert(
            slot_idx,
            SlotHeader {
                fd: raw,
                epoch,
                armed: true,
                ud,
            },
        );
        self.arm_read_multi(raw, Udata::from_token(ud, TAG_RECV)).is_ok()
    }

    unsafe fn arm_recv_msg_multi_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        msg: *const libc::msghdr,
    ) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Recv {
                ud,
                result: -libc::EBADF,
                more: false,
                bid: None,
            });
            return true;
        };
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        let epoch = ud.epoch().raw();
        self.recvmsg_slots.insert(
            slot_idx,
            RecvMsgSlot {
                hdr: SlotHeader { fd: raw, epoch, armed: true, ud },
                msg_template: msg,
            },
        );
        self.arm_read_multi(raw, Udata::from_token(ud, TAG_RECV_MSG)).is_ok()
    }

    fn submit_send_tagged_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        ptr: *const u8,
        len: u32,
    ) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        if self.write_retry_fd.contains_key(&raw) {
            return false;
        }
        // SAFETY: `raw` is a live socket fd; `ptr`/`len` are valid for this call.
        let n = unsafe { libc::send(raw, ptr.cast(), len as usize, 0) };
        if n >= 0 {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: n as i32,
            });
            return true;
        }
        let errno = Errno::last_raw();
        if Errno::is_block_raw(errno) {
            return self.arm_write_retry(raw, ud, WriteKind::Send { ptr, len });
        }
        self.push_pending(PendingCompletion::Write {
            ud,
            result: -errno,
        });
        true
    }

    /// Queue a synchronous file-op result: a negative syscall return becomes
    /// `-errno`, anything else the byte/fd count. Call before any other libc
    /// call so `errno` still reflects the failing syscall.
    fn complete_io(&mut self, ud: Token, rc: isize) -> bool {
        let result = if rc < 0 { -Errno::last_raw() } else { rc as i32 };
        self.push_pending(PendingCompletion::Write { ud, result });
        true
    }

    fn submit_write_fd_inner(
        &mut self,
        ud: Token,
        fd: RawFd,
        ptr: *const u8,
        len: u32,
        offset: u64,
    ) -> bool {
        // SAFETY: `fd` is a live file fd owned by the caller; `ptr`/`len` are valid for this call.
        let n = unsafe { libc::pwrite(fd, ptr.cast(), len as usize, offset as libc::off_t) };
        self.complete_io(ud, n)
    }

    fn submit_fsync_inner(&mut self, ud: Token, fd: RawFd) -> bool {
        // SAFETY: `fd` is a live file fd owned by the caller.
        let rc = unsafe { libc::fsync(fd) };
        self.complete_io(ud, rc as isize)
    }

    fn submit_openat_inner(
        &mut self,
        ud: Token,
        dir: RawFd,
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        fixed: Option<FdSlot>,
    ) -> bool {
        // SAFETY: `path` is a valid NUL-terminated C string owned by the caller for this call.
        // A fixed open keeps the fd in the driver table, so force `O_CLOEXEC` (the
        // manifold strips it for io_uring, which registers the fd out of band).
        let oflag = if fixed.is_some() { flags | libc::O_CLOEXEC } else { flags };
        let fd = unsafe { libc::openat(dir, path, oflag, mode as libc::c_uint) };
        if fd < 0 {
            return self.complete_io(ud, fd as isize);
        }
        // io_uring reports a fixed open as result 0 (the fd lands in the slot, not the CQE).
        if let Some(slot) = fixed {
            let _ = self.register_raw_fd(slot.raw(), fd);
            return self.complete_io(ud, 0);
        }
        self.complete_io(ud, fd as isize)
    }

    fn submit_read_inner(&mut self, ud: Token, fd: RawFd, ptr: *mut u8, len: u32, offset: u64) -> bool {
        // SAFETY: `fd` is live; `ptr`/`len` name a buffer valid for this call.
        let n = unsafe { libc::pread(fd, ptr.cast(), len as usize, offset as libc::off_t) };
        self.complete_io(ud, n)
    }

    fn submit_splice_inner(
        &mut self,
        ud: Token,
        fd_in: RawFd,
        off_in: i64,
        fd_out: RawFd,
        off_out: i64,
        len: u32,
    ) -> bool {
        // macOS has no splice(2); bounce one chunk through a buffer. A negative
        // offset means the fd has no seek position (pipe/socket), so use plain
        // read/write; otherwise the positioned variants. The caller advances by
        // the returned count, so a short move is re-driven.
        let cap = (len as usize).min(SPLICE_BOUNCE);
        let mut buf = vec![0u8; cap];
        let n = unsafe {
            if off_in < 0 {
                libc::read(fd_in, buf.as_mut_ptr().cast(), cap)
            } else {
                libc::pread(fd_in, buf.as_mut_ptr().cast(), cap, off_in as libc::off_t)
            }
        };
        if n <= 0 {
            return self.complete_io(ud, n);
        }
        let w = unsafe {
            if off_out < 0 {
                libc::write(fd_out, buf.as_ptr().cast(), n as usize)
            } else {
                libc::pwrite(fd_out, buf.as_ptr().cast(), n as usize, off_out as libc::off_t)
            }
        };
        self.complete_io(ud, w)
    }

    fn submit_socket_at(
        &mut self,
        domain: i32,
        socket_type: i32,
        protocol: i32,
        slot: FdSlot,
        ud: Token,
    ) -> bool {
        // SAFETY: socket(2) with caller-provided domain/type/protocol; result is checked.
        let raw = unsafe { libc::socket(domain, socket_type, protocol) };
        if raw < 0 {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: -Errno::last_raw(),
            });
            return true;
        }
        let sock = OsFd::take(raw);
        let result = if sock.set_cloexec().is_err() || sock.set_nonblocking().is_err() {
            -Errno::last_raw()
        } else {
            let raw = sock.into_raw_fd();
            match self.register_raw_fd(slot.raw(), raw) {
                Ok(()) => 0,
                Err(_) => {
                    self.close_raw(raw);
                    -libc::EMFILE
                }
            }
        };
        self.push_pending(PendingCompletion::Write {
            ud,
            result,
        });
        true
    }

    fn submit_connect(
        &mut self,
        slot: FdSlot,
        addr_ptr: *const libc::sockaddr,
        addr_len: u32,
        ud: Token,
    ) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        // SAFETY: raw is a live socket; addr_ptr/addr_len are the caller-provided peer address.
        let rc = unsafe { libc::connect(raw, addr_ptr, addr_len as libc::socklen_t) };
        if rc == 0 {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: 0,
            });
            return true;
        }
        let errno = Errno::last_raw();
        if errno == libc::EINPROGRESS || Errno::is_block_raw(errno) {
            return self.arm_write_retry(raw, ud, WriteKind::Connect { addr_ptr, addr_len });
        }
        self.push_pending(PendingCompletion::Write {
            ud,
            result: -errno,
        });
        true
    }

    unsafe fn submit_send_msg_tagged_inner(
        &mut self,
        ud: Token,
        slot: FdSlot,
        msg: *const libc::msghdr,
    ) -> bool {
        let Some(raw) = self.raw_fd(slot) else {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: -libc::EBADF,
            });
            return true;
        };
        if self.write_retry_fd.contains_key(&raw) {
            return false;
        }
        // SAFETY: `raw` is a live socket fd; `msg` is valid because `submit_send_msg_tagged_inner` is `unsafe fn` with that contract.
        let n = unsafe { libc::sendmsg(raw, msg, 0) };
        if n >= 0 {
            self.push_pending(PendingCompletion::Write {
                ud,
                result: n as i32,
            });
            return true;
        }
        let errno = Errno::last_raw();
        if Errno::is_block_raw(errno) {
            return self.arm_write_retry(raw, ud, WriteKind::SendMsg { msg });
        }
        self.push_pending(PendingCompletion::Write {
            ud,
            result: -errno,
        });
        true
    }

    fn cancel_recv_inner(&mut self, ud: Token) {
        let slot_idx = Udata::slot_key(ud.route(), ud.slot().raw());
        if let Some(slot) = self.recv_slots.remove(&slot_idx) {
            self.disarm_filter(slot.fd, libc::EVFILT_READ);
            self.push_pending(PendingCompletion::Recv {
                ud: slot.ud,
                result: -libc::ECANCELED,
                more: false,
                bid: None,
            });
        }
        if let Some(slot) = self.recvmsg_slots.remove(&slot_idx) {
            self.disarm_filter(slot.hdr.fd, libc::EVFILT_READ);
            self.push_pending(PendingCompletion::Recv {
                ud: slot.hdr.ud,
                result: -libc::ECANCELED,
                more: false,
                bid: None,
            });
        }
    }
}

impl Driver {
    fn boot_register(&mut self, handle: OsFd) -> io::Result<u32> {
        let slot = self.alloc_fixed_range(1)?;
        self.register_raw_fd(slot, handle.into_raw_fd())?;
        Ok(slot)
    }
}

impl crate::backend::Bootstrap for Driver {
    fn bind_listener_slot(
        &mut self,
        addr: SocketAddr,
        backlog: i32,
        opts: &ListenerOpts,
    ) -> io::Result<(Fd, SocketAddr)> {
        let handle = OsFd::open(Domain::for_addr(&addr), Kind::Stream)?;
        handle.apply_reuse(opts)?;
        handle.bind(&Addr::from_std(addr))?;
        handle.listen(backlog)?;
        let actual = handle.local_addr()?;
        let idx = self.boot_register(handle)?;
        Ok((self.adopt_fd_raw(idx), actual))
    }

    fn bind_datagram_slot(&mut self, addr: SocketAddr) -> io::Result<(Fd, SocketAddr)> {
        let handle = OsFd::open(Domain::for_addr(&addr), Kind::Dgram)?;
        handle.set_nonblocking()?;
        handle.apply_reuse(&crate::backend::datagram_opts(&addr))?;
        handle.bind(&Addr::from_std(addr))?;
        let actual = handle.local_addr()?;
        let idx = self.boot_register(handle)?;
        Ok((self.adopt_fd_raw(idx), actual))
    }
}

impl crate::backend::Backend for Backend {
    type Driver = Driver;
    type Config = config::Config;

    fn new_driver(cfg: Self::Config) -> io::Result<Self::Driver> {
        Driver::new(cfg)
    }

    fn init_process(_cfg: &Config) -> io::Result<()> {
        // SAFETY: SIGPIPE/SIG_IGN is always safe to set; thread-per-core model has no async-signal concerns here.
        unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
        Ok(())
    }

    fn init_thread(_cpu_id: u16) -> io::Result<()> {
        Ok(())
    }
}

