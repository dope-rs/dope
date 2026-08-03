pub(crate) mod files;

use crate::backend::uring::sqe;

use std::io::{self, Error, ErrorKind};
use std::os::fd::AsRawFd;
use std::process::abort;

use io_uring::IoUring;

use self::files::FileTable;
use crate::backend::fixed::FixedSlots;
use crate::backend::uring::provided::ffi::ring::{Buffer, ProvidedRing, RegisteredRing};
use crate::driver::route::Routes;
use crate::driver::token::{
    KIND_SHIFT, KeyTag, ROUTE_FRAMEWORK, SLOT_MASK, Token, TokenCapacity, TokenSlab,
};
use crate::driver::Config;
use crate::io::fd::FdSlot;
use crate::io::{BUFFER, BUFFER_SHIFT};
use io_uring::types::{CancelBuilder, Timespec};
use sqe::Sqe;

use super::platform::gso::Gso;
use super::raw::submission::Submission;
use crate::platform::raw::host::HOST;
use crate::io::file::RawMetadata;
use crate::platform::Platform;
use crate::platform::snapshot::Snapshot;
use crate::driver::token::kind::ACCEPT;
use crate::driver::token::kind::CLOSE;
use crate::driver::token::kind::CLOSE_PREP;
use crate::driver::token::kind::CREATE;
use libc::EINVAL;
use crate::driver::token::kind::OPEN;
use crate::driver::token::kind::RECV;
use crate::driver::token::kind::SETSOCKOPT;
use libc::SYS_io_uring_register;
use libc::c_int;
use libc::c_long;
use libc::close;
use libc::statx;
use libc::syscall;

const SETSOCKOPT_CAP: usize = 4096;
const _: () = assert!(SETSOCKOPT_CAP <= SLOT_MASK as usize + 1);
type SetsockoptTag = KeyTag<ROUTE_FRAMEWORK, { SETSOCKOPT }>;
pub(crate) enum Disposition {
    Drop,
    DropBuffer(Buffer),
    Internal,
    Public(Token),
}

pub struct Uring {
    pub(crate) ring: RegisteredRing,
    pub(crate) setsockopt: TokenSlab<c_int, SetsockoptTag>,
    pub(crate) files: FileTable,
    fixed_slots: FixedSlots,
    pub(crate) routes: Routes,
}

struct RingSetup {
    cq_entries: u32,
    ring_entries: u32,
    defer_taskrun: bool,
}

impl RingSetup {
    fn new(cq_entries: u32, defer_taskrun: bool, ring_entries: u32) -> Self {
        Self {
            cq_entries,
            ring_entries,
            defer_taskrun,
        }
    }

    fn attempt(
        &self,
        no_sqarray: bool,
        single_issuer: bool,
        defer_taskrun: bool,
    ) -> io::Result<IoUring> {
        let mut builder = IoUring::builder();
        builder.setup_submit_all();
        if single_issuer {
            builder.setup_single_issuer();
        }
        if no_sqarray {
            builder.setup_no_sqarray();
        }
        builder.setup_cqsize(self.cq_entries);
        if defer_taskrun {
            builder.setup_defer_taskrun();
        }
        builder.build(self.ring_entries)
    }

    fn build(&self) -> io::Result<IoUring> {
        let may_fallback = |error: &Error| error.raw_os_error() == Some(EINVAL);
        let mut built = self.attempt(true, true, self.defer_taskrun);
        if built.as_ref().err().is_some_and(may_fallback) {
            built = self.attempt(false, true, self.defer_taskrun);
        }
        if built.as_ref().err().is_some_and(may_fallback) {
            built = self.attempt(false, false, false);
        }
        let uring = built?;

        if !uring.params().is_feature_ext_arg() {
            return Err(Error::new(ErrorKind::Unsupported, "IORING_FEAT_EXT_ARG"));
        }
        if !uring.params().is_feature_nodrop() {
            return Err(Error::new(ErrorKind::Unsupported, "IORING_FEAT_NODROP"));
        }
        Ok(uring)
    }

    fn register_alloc_range(ring: &IoUring, len: u32) -> io::Result<()> {
        #[repr(C)]
        struct Range {
            off: u32,
            len: u32,
            resv: u64,
        }
        const FILE_ALLOC_RANGE: c_long = 25;
        let range = Range {
            off: 0,
            len,
            resv: 0,
        };
        let rc = unsafe {
            syscall(
                SYS_io_uring_register,
                AsRawFd::as_raw_fd(ring) as c_long,
                FILE_ALLOC_RANGE,
                &raw const range as usize as c_long,
                0 as c_long,
            )
        };
        if rc < 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }
}

impl Uring {
    pub(crate) fn new(cfg: &Config) -> io::Result<(Self, usize)> {
        let uring = RingSetup::new(cfg.cq_entries, cfg.defer_taskrun, cfg.ring_entries).build()?;
        match uring
            .submitter()
            .register_sync_cancel(None, CancelBuilder::user_data(u64::MAX).all())
        {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        uring
            .submitter()
            .register_files_sparse(cfg.fixed_file_slots)?;
        RingSetup::register_alloc_range(&uring, cfg.accept_slots)?;
        let Some(setsockopt_capacity) = TokenCapacity::new(SETSOCKOPT_CAP) else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: setsockopt capacity exceeds token slots",
            ));
        };

        let slots = cfg.fixed_file_slots as usize;
        let fixed_slots = FixedSlots::new(cfg.accept_slots, cfg.fixed_file_slots)?;
        let setsockopt = TokenSlab::with_capacity(setsockopt_capacity);
        let files = FileTable::new(slots);
        let routes = Routes::new();
        let ring = RegisteredRing::new(uring, cfg.recv.entries, cfg.recv.len)?;
        Ok((
            Uring {
                ring,
                setsockopt,
                files,
                fixed_slots,
                routes,
            },
            slots,
        ))
    }
}

impl Drop for Uring {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Uring {
    pub(crate) fn shutdown(&mut self) {
        let _ = self.ring.io_mut().submit();
        match self
            .ring
            .io()
            .submitter()
            .register_sync_cancel(None, CancelBuilder::any())
        {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => abort(),
        }
        loop {
            let mut drained = false;
            {
                let Self {
                    ring,
                    setsockopt,
                    files,
                    routes,
                    ..
                } = self;
                let (uring, provided) = ring.split();
                let mut cq = uring.completion();
                for item in cq.by_ref() {
                    drained = true;
                    if let Disposition::DropBuffer(bid) = Self::complete_cqe(
                        provided,
                        setsockopt,
                        files,
                        routes,
                        item.user_data(),
                        item.result(),
                        item.flags(),
                    ) {
                        provided.defer(bid);
                    }
                }
                cq.sync();
            }
            if !drained {
                break;
            }
            let _ = self.ring.io_mut().submit();
        }
    }

    pub(crate) fn await_one(&mut self) -> io::Result<i32> {
        loop {
            self.ring.io().submitter().submit_and_wait(1)?;
            if let Some(result) = self.drain_bootstrap_completions() {
                return Ok(result);
            }
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

    pub(crate) fn await_fixed_range_empty(&mut self, base: u32, len: u32) -> io::Result<()> {
        while !self.files.range_empty(base, len) {
            self.ring.io().submitter().submit_and_wait(1)?;
            self.drain_bootstrap_completions();
        }
        Ok(())
    }

    fn drain_bootstrap_completions(&mut self) -> Option<i32> {
        let mut found = None;
        {
            let Self {
                ring,
                setsockopt,
                files,
                routes,
                ..
            } = self;
            let (uring, provided) = ring.split();
            let mut cq = uring.completion();
            for item in cq.by_ref() {
                let result = item.result();
                match Self::complete_cqe(
                    provided,
                    setsockopt,
                    files,
                    routes,
                    item.user_data(),
                    result,
                    item.flags(),
                ) {
                    Disposition::Drop => {}
                    Disposition::DropBuffer(buffer) => provided.defer(buffer),
                    Disposition::Internal | Disposition::Public(_) => {
                        found = Some(result);
                    }
                }
            }
            cq.sync();
        }
        self.flush_deferred_close();
        self.flush_ready_create();
        found
    }

    fn release_setsockopt(
        setsockopt: &mut TokenSlab<c_int, SetsockoptTag>,
        token: Token,
    ) -> bool {
        let Some(parts) = token.parts::<SetsockoptTag>() else {
            return false;
        };
        setsockopt.remove_parts(parts);
        true
    }

    pub(crate) fn complete_cqe(
        provided: &ProvidedRing,
        setsockopt: &mut TokenSlab<c_int, SetsockoptTag>,
        files: &mut FileTable,
        routes: &Routes,
        user_data: u64,
        result: i32,
        flags: u32,
    ) -> Disposition {
        let Some(token) = Token::try_from_raw(user_data) else {
            return if flags & BUFFER != 0 {
                Disposition::DropBuffer(provided.buffer_from_cqe(
                    (flags >> BUFFER_SHIFT) as u16,
                ))
            } else {
                Disposition::Drop
            };
        };
        if Self::release_setsockopt(setsockopt, token) {
            return Disposition::Internal;
        }
        let op_kind = (user_data >> KIND_SHIFT) as u8;
        if token.route() == ROUTE_FRAMEWORK {
            return match op_kind {
                CLOSE_PREP => Disposition::Drop,
                CLOSE => {
                    files.complete_close(token.slot());
                    Disposition::Drop
                }
                CREATE => files
                    .complete_create(token.slot(), result)
                    .map_or(Disposition::Drop, Disposition::Public),
                _ => Disposition::Public(token),
            };
        }
        if op_kind == ACCEPT {
            let poisoned = routes.is_poisoned(token.route());
            files.mark_accepted(result, poisoned);
            if poisoned {
                return Disposition::Drop;
            }
        } else if op_kind == OPEN && result >= 0 && routes.is_poisoned(token.route()) {
            unsafe {
                close(result);
            }
            return Disposition::Drop;
        } else if op_kind == RECV && flags & BUFFER != 0 && routes.is_poisoned(token.route())
        {
            return Disposition::DropBuffer(provided.buffer_from_cqe(
                (flags >> BUFFER_SHIFT) as u16,
            ));
        }
        Disposition::Public(token)
    }

    pub(crate) fn flush_deferred_close(&mut self) {
        let Self { ring, files, .. } = self;
        let uring = ring.io_mut();
        files.flush_deferred_close(|slot| Submission::try_close(uring, slot));
    }

    pub(crate) fn flush_ready_create(&mut self) {
        let Self { ring, files, .. } = self;
        let uring = ring.io_mut();
        files.flush_ready(|sqe| Submission::push(uring, sqe).is_ok());
    }

    pub(crate) fn close_fd(&mut self, slot: FdSlot) {
        let Self { ring, files, .. } = self;
        let uring = ring.io_mut();
        files.release(slot, |slot| {
            Submission::try_close(uring, slot)
                || (uring.submit().is_ok() && Submission::try_close(uring, slot))
        });
    }
}

impl Platform for Uring {
    type Sqe = Sqe;
    type Gso = Gso;
    type StatBuf = statx;
    type TimerSpec = Timespec;

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
