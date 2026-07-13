mod backend;
mod bootstrap;
mod datagram;
mod drive;
mod lend;
mod parker;
mod sockopt;

mod config;
mod init;
pub mod platform;
pub(super) mod provided;
pub mod sqe;
pub(super) mod system;

pub(super) use backend::Backend;
pub(super) use config::Config;

use std::collections::VecDeque;
use std::io;
use std::cell::UnsafeCell;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;

use io_uring::IoUring;

use crate::slab::Slab;

use crate::backend::ListenerOpts;
use crate::backend::os_fd::OsFd;
use crate::backend::token::{Epoch, LocalIdx, ROUTE_FRAMEWORK, ROUTE_SHIFT, KIND_SHIFT, Token, kind};
use crate::backend::socket::{Addr, Domain, FdSlot, Kind};

const SETSOCKOPT_CAP: usize = 4096;
const DEFERRED_CLOSE_CAP: usize = 4096;
const BOOT_UD: Token = Token::new(ROUTE_FRAMEWORK, LocalIdx::new(0), Epoch::ZERO);

/// A single-threaded reactor with many live handles into it — session currency,
/// [`socket::Fd`] backrefs, waker slots — so mutability is interior, never `&mut`:
/// every handle is a plain `&Driver`.
///
/// ```text
/// &'d Driver ── enter() borrow: Fd<'d> ⊂ exec, proven by borrowck
/// ```
pub struct Driver {
    inner: UnsafeCell<Inner>,
    /// Leaked on purpose: wakers are `'static`-shaped (`std::task::Waker`) and
    /// may outlive the driver; a late wake lands in leaked memory and is
    /// epoch-filtered, never a use-after-free.
    arena: &'static crate::backend::park::Arena,
}

pub(super) struct Inner {
    /// Must drop before `provided`: closing the ring cancels in-flight ops first.
    uring: IoUring,
    setsockopt: Slab<Box<libc::c_int>>,
    deferred_close: VecDeque<FdSlot>,
    provided: provided::Ring,
    next_slot: u32,
    accept_slots: u32,
    defer_active: bool,
}

impl Drop for Driver {
    fn drop(&mut self) {
        crate::memstats::dump();
    }
}

impl Driver {
    pub fn new(cfg: Config) -> io::Result<Self> {
        <Backend as crate::backend::Backend>::init_process(&cfg)?;
        let init::Built { uring, defer_active } =
            init::Setup::new(cfg.cq_entries, cfg.defer_taskrun, cfg.ring_entries).build()?;
        uring.submitter().register_files_sparse(cfg.fixed_file_slots)?;
        init::Setup::register_alloc_range(&uring, cfg.accept_slots)?;
        let provided = provided::Ring::new(
            &uring.submitter(),
            cfg.provided_buf_entries,
            cfg.provided_buf_len,
        )?;

        let slots = cfg.fixed_file_slots as usize;
        Ok(Self {
            inner: UnsafeCell::new(Inner {
                setsockopt: Slab::new(SETSOCKOPT_CAP),
                deferred_close: VecDeque::new(),
                uring,
                provided,
                next_slot: cfg.fixed_file_slots,
                accept_slots: cfg.accept_slots,
                defer_active,
            }),
            arena: Box::leak(crate::backend::park::Arena::new(slots)?),
        })
    }

    /// # Safety
    /// Thread-confined, and every public method is a leaf: it takes this once
    /// at entry, never calls back out while holding it, and drops it on return.
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    pub(super) unsafe fn inner(&self) -> &mut Inner {
        unsafe { &mut *self.inner.get() }
    }

    #[inline]
    pub(super) fn arena(&self) -> &'static crate::backend::park::Arena {
        self.arena
    }

    pub fn defer_active(&self) -> bool {
        // SAFETY: leaf read.
        unsafe { self.inner() }.defer_active
    }

    /// Delegating shims for public traits live below; the real bodies sit on
    /// [`Inner`].
    pub fn reserve_outbound(&self, count: u32) -> io::Result<crate::backend::OutboundReservation> {
        // SAFETY: leaf.
        let base = unsafe { self.inner() }.alloc_fixed_range(count)?;
        Ok(crate::backend::OutboundReservation::new(base, count))
    }

    #[inline]
    pub(crate) fn release_fd_slot(&self, slot: FdSlot) {
        // SAFETY: leaf.
        unsafe { self.inner() }.release_fd_slot(slot);
    }
}

impl Inner {
    #[inline]
    fn push_sqe(&mut self, sqe: sqe::Sqe) -> Result<(), crate::backend::PushError> {
        let entry = sqe.entry();
        if self.try_push(entry).is_ok() {
            return Ok(());
        }
        self.uring.submit().map_err(|_| crate::backend::PushError)?;
        self.try_push(entry)
    }

    fn set_sockopt(
        &mut self,
        fixed_idx: u32,
        level: u32,
        optname: u32,
        value: i32,
    ) -> Result<(), crate::backend::PushError> {
        let Some(key) = self.setsockopt.alloc(Box::new(value)) else {
            return Err(crate::backend::PushError);
        };
        let optval_ptr =
            (&**self.setsockopt.get(key).unwrap() as *const libc::c_int).cast::<libc::c_void>();
        let ud = Token::from_key(ROUTE_FRAMEWORK, key);
        let sqe = io_uring::opcode::SetSockOpt::new(
            io_uring::types::Fixed(fixed_idx),
            level,
            optname,
            optval_ptr,
            size_of::<libc::c_int>() as u32,
        )
        .build()
        .user_data(ud.raw());
        if self.try_push(&sqe).is_ok() {
            Ok(())
        } else {
            self.setsockopt.remove(key);
            Err(crate::backend::PushError)
        }
    }

    #[inline]
    fn try_push(&mut self, sqe: &io_uring::squeue::Entry) -> Result<(), crate::backend::PushError> {
        // SAFETY: `push` only copies the entry; buffers it references are pinned by the in-flight request.
        unsafe { self.uring.submission().push(sqe) }.map_err(|_| crate::backend::PushError)
    }

    #[inline]
    fn try_push_pair(
        &mut self,
        first: &io_uring::squeue::Entry,
        second: &io_uring::squeue::Entry,
    ) -> Result<(), crate::backend::PushError> {
        let entries = [first.clone(), second.clone()];
        // SAFETY: `push_multiple` only copies entries; their referenced buffers (none here) outlive use.
        unsafe { self.uring.submission().push_multiple(&entries) }.map_err(|_| crate::backend::PushError)
    }

    pub(super) fn await_one(&mut self) -> io::Result<i32> {
        loop {
            self.uring.submitter().submit_and_wait(1)?;
            let Self { uring, setsockopt, .. } = self;
            let mut cq = uring.completion();
            let mut found = None;
            for item in cq.by_ref() {
                Self::release_setsockopt(setsockopt, item.user_data());
                found = Some(item.result());
            }
            cq.sync();
            if let Some(result) = found {
                return Ok(result);
            }
        }
    }

    pub(super) fn alloc_fixed_range(&mut self, len: u32) -> io::Result<u32> {
        let base = self
            .next_slot
            .checked_sub(len)
            .filter(|&b| b >= self.accept_slots)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::OutOfMemory, "dope: fixed-file slots exhausted")
            })?;
        self.next_slot = base;
        Ok(base)
    }

    #[inline]
    fn release_setsockopt(setsockopt: &mut Slab<Box<libc::c_int>>, raw: u64) -> bool {
        let route = (raw >> ROUTE_SHIFT) as u8;
        let kind_v = (raw >> KIND_SHIFT) as u8;
        if route != ROUTE_FRAMEWORK || kind_v != kind::SETSOCKOPT {
            return false;
        }
        let key = Token::from_raw(raw).key();
        if let Some(epoch) = setsockopt.epoch(key.index())
            && epoch == key.epoch()
        {
            setsockopt.remove(key);
        }
        true
    }

    #[inline]
    fn push_close(&mut self, slot: FdSlot) -> bool {
        let shut = sqe::Sqe::shutdown_linked_at(slot, libc::SHUT_RDWR);
        let close = sqe::Sqe::close_at(slot);
        self.try_push_pair(shut.entry(), close.entry()).is_ok()
    }

    fn flush_deferred_close(&mut self) {
        while let Some(&slot) = self.deferred_close.front() {
            if !self.push_close(slot) {
                break;
            }
            self.deferred_close.pop_front();
        }
    }

    pub(super) fn release_fd_slot(&mut self, slot: FdSlot) {
        self.flush_deferred_close();
        if self.push_close(slot) {
            return;
        }
        if self.uring.submit().is_ok() && self.push_close(slot) {
            return;
        }
        if self.deferred_close.len() < DEFERRED_CLOSE_CAP {
            self.deferred_close.push_back(slot);
            return;
        }
        loop {
            let _ = self.uring.submitter().submit_and_wait(1);
            if self.push_close(slot) {
                return;
            }
            if self.deferred_close.len() < DEFERRED_CLOSE_CAP {
                self.deferred_close.push_back(slot);
                return;
            }
        }
    }

    fn boot_await(&mut self, min: i32, fallback: i32) -> io::Result<()> {
        let rc = self.await_one()?;
        if rc < min {
            return Err(io::Error::from_raw_os_error(if rc < 0 { -rc } else { fallback }));
        }
        Ok(())
    }

    fn boot_perform(&mut self, sqe: sqe::Sqe) -> io::Result<()> {
        self.push_sqe(sqe)?;
        self.boot_await(0, 0)
    }

    fn boot_bind_slot(
        &mut self,
        domain: Domain,
        kind: Kind,
        addr: SocketAddr,
        opts: Option<&ListenerOpts>,
    ) -> io::Result<u32> {
        let idx = self.alloc_fixed_range(1)?;
        let slot = FdSlot::new(idx);
        self.boot_perform(sqe::Sqe::socket_at(domain.raw(), kind.raw(), 0, slot, BOOT_UD)?)?;
        if let Some(opts) = opts {
            self.boot_apply_opts(idx, opts)?;
        }
        let bound = Addr::from_std(addr);
        self.boot_perform(sqe::Sqe::bind_at(slot, bound.ptr(), bound.socklen(), BOOT_UD))?;
        Ok(idx)
    }

    fn boot_apply_opts(&mut self, slot: u32, opts: &ListenerOpts) -> io::Result<()> {
        if opts.reuse_addr {
            self.boot_setsockopt(slot, libc::SOL_SOCKET as u32, libc::SO_REUSEADDR as u32, 1)?;
        }
        if opts.reuse_port {
            self.boot_setsockopt(slot, libc::SOL_SOCKET as u32, libc::SO_REUSEPORT as u32, 1)?;
        }
        if let Some(qlen) = opts.fastopen_backlog {
            self.boot_setsockopt(slot, libc::IPPROTO_TCP as u32, libc::TCP_FASTOPEN as u32, qlen as i32)?;
        }
        if let Some(secs) = opts.defer_accept_secs {
            self.boot_setsockopt(slot, libc::IPPROTO_TCP as u32, libc::TCP_DEFER_ACCEPT as u32, secs as i32)?;
        }
        Ok(())
    }

    fn boot_setsockopt(&mut self, slot: u32, level: u32, optname: u32, value: i32) -> io::Result<()> {
        self.set_sockopt(slot, level, optname, value)?;
        self.boot_await(0, 0)
    }

    fn boot_register_raw(&mut self, raw: std::os::fd::RawFd, slot: u32) -> io::Result<()> {
        let mut fds = [raw];
        let entry = io_uring::opcode::FilesUpdate::new(fds.as_mut_ptr().cast_const(), 1)
            .offset(slot as i32)
            .build()
            .user_data(BOOT_UD.raw());
        self.push_sqe(sqe::Sqe::from_entry(entry))?;
        self.boot_await(1, libc::EMFILE)
    }

    fn boot_bound_via_syscall(
        &mut self,
        addr: SocketAddr,
        kind: Kind,
        opts: &ListenerOpts,
        backlog: Option<i32>,
    ) -> io::Result<(u32, SocketAddr)> {
        let handle = OsFd::open(Domain::for_addr(&addr), kind)?;
        handle.apply_reuse(opts)?;
        handle.bind(&Addr::from_std(addr))?;
        match backlog {
            Some(backlog) => handle.listen(backlog)?,
            None => handle.set_nonblocking()?,
        }
        let actual = handle.local_addr()?;
        let slot = self.alloc_fixed_range(1)?;
        self.boot_register_raw(handle.as_raw_fd(), slot)?;
        drop(handle);
        Ok((slot, actual))
    }
}