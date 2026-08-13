mod admissions;
mod sealed;

use core::time;
use std::{io, mem, os::fd, process};

use io_uring::{cqueue, squeue, types};

use crate::{backend::uring, driver::settings, io::fd::handles};

/// Field order closes the kernel ring before releasing kernel-visible memory.
struct Raw {
    io: io_uring::IoUring,
    provided: uring::ffi::ProvidedRing,
    taskrun_flag: bool,
}

/// Production ring paired with its task-local registered enter authority.
struct RegisteredRaw {
    ring: Owner,
    provided: uring::ffi::ProvidedRing,
    enter: uring::ffi::RegisteredEnter,
    taskrun_flag: bool,
    completion_drain_active: bool,
}

/// A fully registered ring whose production contract has not yet been admitted.
#[repr(transparent)]
pub(super) struct Candidate(Raw);

/// The only ring state exposed to the production backend.
#[repr(transparent)]
pub(in crate::backend::uring) struct Ready(RegisteredRaw);

/// Armed direct access to the synchronized completion cursor.
pub(in crate::backend::uring) struct Drain;

pub(crate) struct Buffers<'a> {
    ring: &'a mut Ready,
}

const _: () = {
    assert!(mem::size_of::<Candidate>() == mem::size_of::<Raw>());
    assert!(mem::align_of::<Candidate>() == mem::align_of::<Raw>());
    assert!(mem::size_of::<Drain>() == 0);
};

pub(in crate::backend::uring) use admissions::Admissions;
pub(in crate::backend::uring::ring) use sealed::{
    Canary, DatagramPair, MultishotCanary, Owner, TcpPair,
};

impl Candidate {
    pub(super) fn build(config: &settings::Config) -> io::Result<Self> {
        let taskrun_flag = matches!(
            config.completion_progress(),
            settings::CompletionProgress::BatchedWhenSupported
        );
        let io = Self::build_io(config, taskrun_flag)?;
        Self::verify_registrations(&io, config.file_slots())?;
        Self::new(io, config.receive(), taskrun_flag)
    }

    fn new(
        io: io_uring::IoUring,
        receive: settings::Receive,
        taskrun_flag: bool,
    ) -> io::Result<Self> {
        let provided = uring::ffi::ProvidedRing::new(&io.submitter(), receive)?;
        Ok(Self(Raw {
            io,
            provided,
            taskrun_flag,
        }))
    }

    fn build_io(config: &settings::Config, taskrun_flag: bool) -> io::Result<io_uring::IoUring> {
        use io_uring::IoUring;

        let queues = config.queue_layout();
        let mut builder = IoUring::builder();
        builder.setup_submit_all();
        builder.setup_single_issuer();
        builder.setup_no_sqarray();
        builder.setup_cqsize(queues.completions());
        if taskrun_flag {
            builder.setup_defer_taskrun();
            builder.setup_taskrun_flag();
        }
        let ring = builder.build(queues.submissions())?;

        Self::require(ring.params().is_feature_ext_arg(), "IORING_FEAT_EXT_ARG")?;
        Self::require(ring.params().is_feature_nodrop(), "IORING_FEAT_NODROP")?;
        Self::require(
            ring.params().is_feature_skip_cqe_on_success(),
            "IORING_FEAT_CQE_SKIP",
        )?;
        Ok(ring)
    }

    fn verify_registrations(
        ring: &io_uring::IoUring,
        slots: settings::FileSlots,
    ) -> io::Result<()> {
        use crate::backend::uring::ffi::fixed;

        match ring
            .submitter()
            .register_sync_cancel(None, types::CancelBuilder::user_data(u64::MAX).all())
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        ring.submitter().register_files_sparse(slots.capacity())?;
        fixed::Fixed::new(slots.accept()).register(ring)
    }

    fn require(supported: bool, feature: &'static str) -> io::Result<()> {
        if supported {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::Unsupported, feature))
        }
    }
}

impl Raw {
    fn update_file(io: &io_uring::IoUring, slot: u32, raw: fd::RawFd) -> io::Result<()> {
        let files = [raw];
        loop {
            match io.submitter().register_files_update(slot, &files) {
                Ok(1) => return Ok(()),
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "dope: incomplete fixed-file update",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl RegisteredRaw {
    fn new(raw: Raw) -> io::Result<Self> {
        let Raw {
            io,
            provided,
            taskrun_flag,
        } = raw;
        let enter = uring::ffi::RegisteredEnter::register(&io)?;
        Ok(Self {
            ring: Owner::new(io),
            provided,
            enter,
            taskrun_flag,
            completion_drain_active: false,
        })
    }

    /// Enters after the caller has published the current SQ tail and refreshed
    /// its head. The full-queue path uses this to avoid repeating that barrier.
    fn submit_synced(&mut self) -> io::Result<usize> {
        if self.completion_drain_active {
            self.ring.completion_mut().sync();
        }
        self.provided.flush();
        self.enter.submit(self.ring.submission())
    }
}

impl Ready {
    pub(crate) fn install_file(
        &self,
        slot: handles::FixedSlot,
        file: fd::BorrowedFd<'_>,
    ) -> io::Result<()> {
        update_file(
            self.0.ring.submitter(),
            slot.raw(),
            fd::AsRawFd::as_raw_fd(&file),
        )
    }

    pub(crate) fn remove_file(&self, slot: handles::FixedSlot) -> io::Result<()> {
        update_file(self.0.ring.submitter(), slot.raw(), -1)
    }

    /// Appends one SQE without entering the kernel. A cached full cursor is
    /// refreshed once before reporting that the shared ring is actually full.
    pub(in crate::backend::uring) fn push_once(
        &mut self,
        entry: &squeue::Entry,
    ) -> Result<(), squeue::PushError> {
        if self.0.ring.push(entry).is_ok() {
            return Ok(());
        }
        self.0.ring.submission_mut().sync();
        self.0.ring.push(entry)
    }

    /// Appends one SQE, flushing a genuinely full ring before retrying.
    pub(in crate::backend::uring) fn push(&mut self, entry: &squeue::Entry) -> io::Result<()> {
        if self.push_once(entry).is_ok() {
            return Ok(());
        }
        self.0.submit_synced()?;
        self.0.ring.submission_mut().sync();
        self.0
            .ring
            .push(entry)
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "dope: io_uring SQ is full"))
    }

    pub(in crate::backend::uring) fn push_multiple_once(
        &mut self,
        entries: &[squeue::Entry],
    ) -> Result<(), squeue::PushError> {
        if self.0.ring.push_multiple(entries).is_ok() {
            return Ok(());
        }
        self.0.ring.submission_mut().sync();
        self.0.ring.push_multiple(entries)
    }

    pub(in crate::backend::uring) fn push_multiple(
        &mut self,
        entries: &[squeue::Entry],
    ) -> io::Result<()> {
        if self.push_multiple_once(entries).is_ok() {
            return Ok(());
        }
        self.0.submit_synced()?;
        self.0.ring.submission_mut().sync();
        self.0
            .ring
            .push_multiple(entries)
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "dope: io_uring SQ is full"))
    }

    pub(crate) fn submit(&mut self) -> io::Result<usize> {
        self.0.ring.submission_mut().sync();
        self.0.submit_synced()
    }

    pub(crate) fn commit(&mut self) -> io::Result<()> {
        let taskrun_flag = self.0.taskrun_flag;
        self.0.ring.submission_mut().sync();
        let (needs_enter, taskrun) = {
            let submission = self.0.ring.submission();
            (
                !submission.is_empty() || submission.cq_overflow(),
                taskrun_flag && submission.taskrun(),
            )
        };
        self.0.provided.flush();
        if taskrun {
            let RegisteredRaw { ring, enter, .. } = &self.0;
            return enter.wait(ring.submission(), Some(time::Duration::ZERO));
        }
        if needs_enter {
            let RegisteredRaw { ring, enter, .. } = &self.0;
            return enter.submit(ring.submission()).map(drop);
        }
        Ok(())
    }

    /// Pulls kernel-retained CQ overflow entries and deferred task work into
    /// the userspace CQ after the visible queue has been drained.
    pub(crate) fn flush_completions(&mut self) -> io::Result<bool> {
        let taskrun_flag = self.0.taskrun_flag;
        let (overflow, taskrun) = {
            let submission = self.0.ring.submission();
            (
                submission.cq_overflow(),
                taskrun_flag && submission.taskrun(),
            )
        };
        if taskrun {
            self.wait(Some(time::Duration::ZERO))?;
            return Ok(true);
        }
        if overflow {
            self.submit()?;
            return Ok(true);
        }
        self.0.provided.flush();
        Ok(false)
    }

    pub(crate) fn wait(&mut self, timeout: Option<time::Duration>) -> io::Result<()> {
        let RegisteredRaw {
            ring,
            provided,
            enter,
            ..
        } = &mut self.0;
        ring.submission_mut().sync();
        provided.flush();
        enter.wait(ring.submission(), timeout)
    }

    pub(crate) fn sync_cancel_all(&mut self) -> io::Result<()> {
        self.0.provided.flush();
        self.0
            .ring
            .submitter()
            .register_sync_cancel(None, types::CancelBuilder::any())
    }

    pub(crate) fn buffers(&mut self) -> Buffers<'_> {
        Buffers { ring: self }
    }
}

impl Drain {
    pub(in crate::backend::uring) fn begin(ring: &mut Ready) -> Self {
        if ring.0.completion_drain_active {
            process::abort();
        }
        ring.0.ring.completion_mut().sync();
        ring.0.completion_drain_active = true;
        Self
    }

    pub(in crate::backend::uring) fn next(&self, ring: &mut Ready) -> Option<cqueue::Entry> {
        if !ring.0.completion_drain_active {
            process::abort();
        }
        ring.0.ring.completion_mut().next()
    }

    pub(in crate::backend::uring) fn finish(self, ring: &mut Ready) -> bool {
        if !ring.0.completion_drain_active {
            process::abort();
        }
        let completion = ring.0.ring.completion_mut();
        completion.sync();
        let pending = !cqueue::CompletionQueue::is_empty(completion);
        ring.0.completion_drain_active = false;
        mem::forget(self);
        pending
    }
}

impl Drop for Drain {
    fn drop(&mut self) {
        eprintln!("dope-core: completion drain ended without publishing its cursor");
        process::abort();
    }
}

impl<'a> Buffers<'a> {
    pub(crate) fn provided(self) -> &'a mut uring::ffi::ProvidedRing {
        &mut self.ring.0.provided
    }
}

impl Drop for Ready {
    fn drop(&mut self) {
        if self.0.completion_drain_active {
            process::abort();
        }
        if self.0.enter.unregister().is_err() {
            process::abort();
        }
    }
}

fn update_file(submitter: &io_uring::Submitter<'_>, slot: u32, raw: fd::RawFd) -> io::Result<()> {
    let files = [raw];
    loop {
        match submitter.register_files_update(slot, &files) {
            Ok(1) => return Ok(()),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "dope: incomplete fixed-file update",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}
