pub(crate) mod pending;
pub(crate) mod read;
pub(crate) mod retry;
pub(crate) mod submit;
pub(crate) mod udata;

use std::io::{self, Error};
use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use o3::collections::FixedQueue;

use crate::backend::fixed::FixedSlots;
use crate::backend::kqueue::recv_pool::ffi::pool::{Backing, Pool};
use crate::driver::Config;
use crate::driver::route::Routes;
use crate::driver::token::{SHUTDOWN, Token};
use crate::io::fd::FdSlot;
use crate::io::file::RawMetadata;
use crate::platform::Platform;
use crate::platform::snapshot::Snapshot;

use self::pending::{PendingCompletion, PendingQueue};
use self::read::{FixedMap, ReadSlot, Resume};
use self::retry::{Retry, WriteRetrySlot};
use self::udata::Udata;
use super::platform::gso::Gso;
use crate::platform::raw::host::HOST;
use super::sqe::{Sqe, TimerSpec};
use libc::uintptr_t;
use std::ptr::{null, null_mut};
use libc::EINTR;
use libc::EVFILT_USER;
use libc::EV_ADD;
use libc::EV_CLEAR;
use libc::EV_ENABLE;
use libc::FD_CLOEXEC;
use libc::F_SETFD;
use libc::NOTE_TRIGGER;
use crate::driver::token::kind::OPEN;
use libc::{c_int, c_long};
use libc::fcntl;
use libc::kevent;
use libc::kqueue;
use libc::stat;
use libc::time_t;
use libc::timespec;

const WAKE_IDENT: uintptr_t = uintptr_t::MAX;

pub(crate) const MAX_DRAIN_PER_FD: usize = 256;
const PENDING_CAP: usize = 1 << 16;
const CHANGES_FLUSH_AT: usize = 4096;
const _: () = assert!(size_of::<Option<OwnedFd>>() <= size_of::<Option<RawFd>>());

pub struct Kqueue {
    pub(crate) kq: OwnedFd,
    pub(crate) changes: Vec<kevent>,
    read_slots: FixedMap<ReadSlot>,
    read_fd: FixedMap<usize>,
    write_retries: Vec<WriteRetrySlot>,
    write_retry_free: Vec<u32>,
    write_retry_fd: FixedMap<u32>,
    pub(crate) resume: FixedQueue<Resume>,
    pub(crate) pending: PendingQueue,
    pub(crate) recv: Pool,
    _backing: Backing,
    fd_table: Vec<Option<OwnedFd>>,
    accept_limit: u32,
    fixed_slots: FixedSlots,
    pub(crate) routes: Routes,
}

impl Kqueue {
    pub(crate) fn new(cfg: &Config) -> io::Result<(Self, usize)> {
        let raw = unsafe { kqueue() };
        if raw < 0 {
            return Err(Error::last_os_error());
        }
        // SAFETY: kqueue returned a fresh owned descriptor.
        let kq = unsafe { OwnedFd::from_raw_fd(raw) };
        let rc = unsafe { fcntl(kq.as_raw_fd(), F_SETFD, FD_CLOEXEC) };
        if rc < 0 {
            return Err(Error::last_os_error());
        }
        let wake = kevent {
            ident: WAKE_IDENT,
            filter: EVFILT_USER,
            flags: EV_ADD | EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: null_mut(),
        };
        let rc = unsafe { kevent(kq.as_raw_fd(), &wake, 1, null_mut(), 0, null()) };
        if rc < 0 {
            return Err(Error::last_os_error());
        }
        let fixed_file_slots = cfg.fixed_file_slots.max(cfg.accept_slots).max(1);
        let slots = fixed_file_slots as usize;
        let accept_limit = cfg.accept_slots.min(fixed_file_slots);
        let (backing, recv) = Backing::allocate(cfg.recv.entries, cfg.recv.len)?;
        Ok((
            Kqueue {
                kq,
                changes: Vec::with_capacity(CHANGES_FLUSH_AT),
                read_slots: FixedMap::with_capacity(slots),
                read_fd: FixedMap::with_capacity(slots),
                write_retries: Vec::with_capacity(slots),
                write_retry_free: Vec::with_capacity(slots),
                write_retry_fd: FixedMap::with_capacity(slots),
                resume: FixedQueue::with_capacity(slots),
                pending: PendingQueue::with_capacity(PENDING_CAP, slots),
                recv,
                _backing: backing,
                fd_table: std::iter::repeat_with(|| None).take(slots).collect(),
                accept_limit,
                fixed_slots: FixedSlots::new(accept_limit, fixed_file_slots)?,
                routes: Routes::new(),
            },
            slots,
        ))
    }
}

impl Kqueue {
    pub(crate) fn shutdown(&mut self) {
        self.clear_write_retries();
        while let Some(completion) = self.pending.pop_front() {
            self.reclaim(completion);
        }
        for index in 0..self.fd_table.len() {
            if let Some(fd) = self.fd_table[index].take() {
                self.close_owned(fd);
            }
        }
        self.changes.clear();
        self.resume.retain(|_| false);
    }

    fn insert_read(&mut self, key: usize, slot: ReadSlot) -> bool {
        let fd = slot.header().fd as usize;
        if self
            .read_fd
            .get(&fd)
            .is_some_and(|registered| *registered != key)
            || self
                .read_slots
                .get(&key)
                .is_some_and(|registered| registered.header().fd as usize != fd)
        {
            return false;
        }
        if self
            .read_slots
            .get(&key)
            .is_some_and(|slot| slot.header().resume_queued)
        {
            self.resume.retain(|resume| resume.key != key);
        }
        let replaced_fd = match self.read_fd.insert(fd, key) {
            Ok(replaced) => replaced,
            Err(_) => return false,
        };
        if self.read_slots.insert(key, slot).is_ok() {
            return true;
        }
        match replaced_fd {
            Some(previous) => {
                let result = self.read_fd.insert(fd, previous);
                debug_assert!(result.is_ok());
            }
            None => {
                self.read_fd.remove(&fd);
            }
        }
        false
    }

    fn remove_read(&mut self, key: usize) -> Option<ReadSlot> {
        let slot = self.read_slots.remove(&key)?;
        if slot.header().resume_queued {
            self.resume.retain(|resume| resume.key != key);
        }
        let fd = slot.header().fd as usize;
        if self
            .read_fd
            .get(&fd)
            .is_some_and(|registered| *registered == key)
        {
            self.read_fd.remove(&fd);
        }
        Some(slot)
    }

    pub(crate) fn reclaim(&mut self, completion: PendingCompletion) {
        match completion {
            PendingCompletion::Accept { result, .. } if result >= 0 => {
                if let Some(slot) = FdSlot::try_from_raw(result as u32) {
                    self.close_fd(slot);
                }
            }
            PendingCompletion::Recv {
                buffer: Some(buffer),
                ..
            } => self.recv.defer(buffer),
            PendingCompletion::Write { ud, result }
                if ud.with_kind(OPEN) == ud && result >= 0 =>
            {
                // SAFETY: successful OPEN returns a fresh owned descriptor.
                self.close_owned(unsafe { OwnedFd::from_raw_fd(result) });
            }
            PendingCompletion::Create {
                slot: Some(slot), ..
            } => self.close_fd(slot),
            _ => {}
        }
    }

    pub(crate) fn quiesce_accept(&mut self, target: Token) {
        let key = Udata::read_key(target);
        if self
            .read_slots
            .get(&key)
            .and_then(ReadSlot::accept)
            .is_some_and(|slot| slot.hdr.ud == target)
        {
            self.remove_read(key);
        }
    }

    pub(crate) fn quiesce_recv(&mut self, target: Token) {
        let key = Udata::read_key(target);
        if self.read_slots.get(&key).is_some_and(|slot| match slot {
            ReadSlot::Recv(slot) => slot.ud == target,
            ReadSlot::RecvMsg(slot) => slot.hdr.ud == target,
            ReadSlot::Accept(_) => false,
        }) {
            self.remove_read(key);
        }
    }

    pub(crate) fn alloc_fixed_range(&mut self, len: u32) -> io::Result<u32> {
        self.fixed_slots.alloc(len)
    }

    pub(crate) fn alloc_fixed_slot(&mut self) -> io::Result<FdSlot> {
        self.fixed_slots.alloc_slot().map(FdSlot::from_index)
    }

    pub(crate) fn retire_fixed_range(&mut self, base: u32, len: u32) {
        let released = self.fixed_slots.release(base, len);
        debug_assert!(released, "dope: invalid fixed-file range retirement");
    }

    pub(crate) fn register_fd(&mut self, slot: u32, fd: OwnedFd) {
        let idx = slot as usize;
        if idx >= self.fd_table.len() {
            self.fd_table.resize_with(idx + 1, || None);
        }
        let cell = &mut self.fd_table[idx];
        if let Some(old) = cell.replace(fd) {
            self.close_owned(old);
        }
    }

    pub(crate) fn raw_fd(&self, slot: FdSlot) -> Option<RawFd> {
        self.fd_table
            .get(slot.raw() as usize)
            .and_then(Option::as_ref)
            .map(AsRawFd::as_raw_fd)
    }

    fn close_owned(&mut self, fd: OwnedFd) {
        let raw = fd.as_raw_fd();
        let mut targets = [SHUTDOWN; 2];
        let mut target_count = 0;
        if let Some(key) = self.read_fd.get(&(raw as usize)).copied()
            && let Some(slot) = self.remove_read(key)
        {
            targets[target_count] = slot.header().ud;
            target_count += 1;
        }
        if let Some(target) = self.cancel_write_retry(raw) {
            targets[target_count] = target;
            target_count += 1;
        }
        self.changes
            .retain(|event| event.ident != raw as uintptr_t);
        let mut extracted = self.pending.extract_targets(&targets[..target_count]);
        while let Some(completion) = self.pending.pop_extracted(&mut extracted) {
            self.reclaim(completion);
        }
    }

    pub(crate) fn close_fd(&mut self, slot: FdSlot) {
        let idx = slot.raw() as usize;
        if let Some(fd) = self.fd_table.get_mut(idx).and_then(Option::take) {
            self.close_owned(fd);
        }
    }

    fn next_accept_slot(&self) -> Option<u32> {
        self.fd_table
            .iter()
            .take(self.accept_limit as usize)
            .position(Option::is_none)
            .map(|idx| idx as u32)
    }

    pub(crate) fn kevent_call(
        &mut self,
        events: &mut [MaybeUninit<kevent>],
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let ts_storage;
        let ts_ptr: *const timespec = match timeout {
            None => null(),
            Some(d) => {
                ts_storage = timespec {
                    tv_sec: d.as_secs() as time_t,
                    tv_nsec: d.subsec_nanos() as c_long,
                };
                &ts_storage
            }
        };
        let n = unsafe {
            kevent(
                self.kq.as_raw_fd(),
                self.changes.as_ptr(),
                self.changes.len() as c_int,
                events.as_mut_ptr().cast(),
                events.len() as c_int,
                ts_ptr,
            )
        };
        self.changes.clear();
        if n < 0 {
            let err = Error::last_os_error();
            return if err.raw_os_error() == Some(EINTR) {
                Ok(0)
            } else {
                Err(err)
            };
        }
        Ok(n as usize)
    }

    pub(crate) fn flush_changes_if_full(&mut self) {
        if self.changes.len() >= CHANGES_FLUSH_AT {
            let _ = self.kevent_call(&mut [], Some(Duration::ZERO));
        }
    }
    pub(crate) fn push_pending(&mut self, c: PendingCompletion) {
        let wake = self.pending.is_empty();
        assert!(
            self.pending.push_back(c),
            "dope-kqueue: pending completion capacity exhausted"
        );
        if wake {
            self.changes.push(kevent {
                ident: WAKE_IDENT,
                filter: EVFILT_USER,
                flags: EV_ENABLE,
                fflags: NOTE_TRIGGER,
                data: 0,
                udata: null_mut(),
            });
            self.flush_changes_if_full();
        }
    }
}

impl Drop for Kqueue {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Platform for Kqueue {
    type Sqe = Sqe;
    type Gso = Gso;
    type StatBuf = stat;
    type TimerSpec = TimerSpec;

    fn entropy() -> io::Result<[u64; 2]> {
        HOST.entropy()
    }

    fn parse_meta(raw: &Self::StatBuf) -> io::Result<RawMetadata> {
        HOST.parse_meta(raw)
    }

    fn snapshot() -> io::Result<Snapshot> {
        HOST.snapshot()
    }
}
