pub(crate) mod pending;
pub(crate) mod read;
pub(crate) mod retry;
pub(crate) mod submit;
pub(crate) mod udata;


use std::io::{self, Error, ErrorKind};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use o3::collections::FixedQueue;

use crate::backend::kqueue::provided::{Backing, Provided};
use crate::driver::Config;
use crate::driver::route::Routes;
use crate::driver::token::{SHUTDOWN, Token, kind};
use crate::io::ffi::Handle;
use crate::io::fd::FdSlot;

use self::pending::{PendingCompletion, PendingQueue};
use self::read::{FixedMap, ReadSlot, Resume};
use self::retry::{Retry, WriteRetrySlot};
use self::udata::Udata;
use libc::uintptr_t;
use std::ptr::{null, null_mut};

const TAG_ACCEPT: u8 = 1;
const TAG_RECV: u8 = 2;
const TAG_RECV_MSG: u8 = 3;
const TAG_WRITE_RETRY: u8 = 4;
pub(crate) const TAG_SHUTDOWN: u8 = 5;

const WAKE_IDENT: uintptr_t = uintptr_t::MAX;

pub(crate) const MAX_DRAIN_PER_FD: usize = 256;
const PENDING_CAP: usize = 1 << 16;
const CHANGES_FLUSH_AT: usize = 4096;
const SPLICE_BOUNCE: usize = 1 << 16;

pub struct Kqueue {
    pub(crate) kq: OwnedFd,
    pub(crate) changes: Vec<libc::kevent>,
    read_slots: FixedMap<ReadSlot>,
    read_fd: FixedMap<usize>,
    write_retries: Vec<WriteRetrySlot>,
    write_retry_free: Vec<u32>,
    write_retry_fd: FixedMap<u32>,
    pub(crate) resume: FixedQueue<Resume>,
    pub(crate) pending: PendingQueue,
    pub(crate) provided: Provided,
    pub(crate) backing: Backing,
    splice_buf: Box<[u8]>,
    fd_table: Vec<Option<RawFd>>,
    accept_limit: u32,
    next_slot: u32,
    pub(crate) routes: Routes,
}

impl Kqueue {
    pub(crate) fn new(cfg: &Config) -> io::Result<(Self, usize)> {
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(Error::last_os_error());
        }
        let kq = unsafe { OwnedFd::from_raw_fd(raw) };
        let rc = unsafe { libc::fcntl(kq.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        if rc < 0 {
            return Err(Error::last_os_error());
        }
        let wake = libc::kevent {
            ident: WAKE_IDENT,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: null_mut(),
        };
        let rc = unsafe { libc::kevent(kq.as_raw_fd(), &wake, 1, null_mut(), 0, null()) };
        if rc < 0 {
            return Err(Error::last_os_error());
        }
        let fixed_file_slots = cfg.fixed_file_slots.max(cfg.accept_slots).max(1);
        let slots = fixed_file_slots as usize;
        let backing = Backing::new(cfg.provided.entries, cfg.provided.len as u32);
        let provided = Provided::new(&backing, cfg.provided.entries);
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
                provided,
                backing,
                splice_buf: vec![0; SPLICE_BOUNCE].into_boxed_slice(),
                fd_table: vec![None; slots],
                accept_limit: cfg.accept_slots.min(fixed_file_slots),
                next_slot: fixed_file_slots,
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
            if let Some(raw) = self.fd_table[index].take() {
                self.close_raw(raw);
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
                self.close_fd(FdSlot::new(result as u32));
            }
            PendingCompletion::Recv { bid: Some(bid), .. } => self.provided.defer(bid),
            PendingCompletion::Write { ud, result }
                if ud.with_kind(kind::OPEN) == ud && result >= 0 =>
            {
                self.close_raw(result);
            }
            PendingCompletion::Create {
                slot: Some(slot), ..
            } => self.close_fd(slot),
            _ => {}
        }
    }

    pub(crate) fn quiesce_accept(&mut self, target: Token) {
        let key = Udata::slot_key(target.route(), target.slot().raw());
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
        let key = Udata::slot_key(target.route(), target.slot().raw());
        if self.read_slots.get(&key).is_some_and(|slot| match slot {
            ReadSlot::Recv(slot) => slot.ud == target,
            ReadSlot::RecvMsg(slot) => slot.hdr.ud == target,
            ReadSlot::Accept(_) => false,
        }) {
            self.remove_read(key);
        }
    }

    pub(crate) fn alloc_fixed_range(&mut self, len: u32) -> io::Result<u32> {
        let base = self
            .next_slot
            .checked_sub(len)
            .filter(|&b| b >= self.accept_limit)
            .ok_or_else(|| {
                Error::new(ErrorKind::OutOfMemory, "dope: fixed-file slots exhausted")
            })?;
        self.next_slot = base;
        Ok(base)
    }

    pub(crate) fn register_raw_fd(&mut self, slot: u32, raw: RawFd) -> io::Result<()> {
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

    pub(crate) fn raw_fd(&self, slot: FdSlot) -> Option<RawFd> {
        self.fd_table.get(slot.raw() as usize).and_then(|v| *v)
    }

    fn close_raw(&mut self, raw: RawFd) {
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
            .retain(|event| event.ident != raw as libc::uintptr_t);
        let mut extracted = self.pending.extract_targets(&targets[..target_count]);
        while let Some(completion) = self.pending.pop_extracted(&mut extracted) {
            self.reclaim(completion);
        }
        drop(Handle::take(raw));
    }

    pub(crate) fn close_fd(&mut self, slot: FdSlot) {
        let idx = slot.raw() as usize;
        if let Some(raw) = self.fd_table.get_mut(idx).and_then(Option::take) {
            self.close_raw(raw);
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
        events: &mut [MaybeUninit<libc::kevent>],
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let ts_storage;
        let ts_ptr: *const libc::timespec = match timeout {
            None => null(),
            Some(d) => {
                ts_storage = libc::timespec {
                    tv_sec: d.as_secs() as libc::time_t,
                    tv_nsec: d.subsec_nanos() as libc::c_long,
                };
                &ts_storage
            }
        };
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
            let err = Error::last_os_error();
            return if err.raw_os_error() == Some(libc::EINTR) {
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
            self.changes.push(libc::kevent {
                ident: WAKE_IDENT,
                filter: libc::EVFILT_USER,
                flags: libc::EV_ENABLE,
                fflags: libc::NOTE_TRIGGER,
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
