use std::os::fd::{FromRawFd, OwnedFd};

use super::FileOutcome;
use dope::DriverContext;
use dope_core::backend::Sqe;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::token::kind::OPEN;
use dope_core::driver::token::{Epoch, KeyTag, ROUTE_FRAMEWORK, SlotIndex, Token, kind};
use dope_core::io::OpenEvent;
use dope_core::io::fd::{Fd, FdSlot};
use dope_core::io::file::OpenPath;
use o3::collections::CellQueue;

use super::table::{OperationTable, Targets};

pub enum OpenDone {
    Direct(OwnedFd),
    Fixed,
    Failed(i32),
}

enum OpenTarget {
    Direct,
    Fixed(FdSlot),
}

struct OpenHold {
    path: OpenPath,
    target: OpenTarget,
}

impl OpenHold {
    fn direct(path: OpenPath) -> Self {
        Self {
            path,
            target: OpenTarget::Direct,
        }
    }

    fn fixed(path: OpenPath, slot: FdSlot) -> Self {
        Self {
            path,
            target: OpenTarget::Fixed(slot),
        }
    }

    fn targets(&self, token: Token) -> Targets {
        match self.target {
            OpenTarget::Fixed(slot) => Targets::two_releasing(
                token,
                Token::new(ROUTE_FRAMEWORK, SlotIndex::new(slot.raw()), Epoch::ZERO)
                    .with_kind(kind::CREATE),
                slot,
            ),
            OpenTarget::Direct => Targets::one(token),
        }
    }
}

pub(crate) struct OpenTable<'d, const ID: u8> {
    operations: OperationTable<'d, OpenHold, OpenDone, KeyTag<ID, OPEN>>,
    releases: CellQueue<FdSlot>,
}

impl<'d, const ID: u8> OpenTable<'d, ID> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            operations: OperationTable::with_capacity(cap),
            releases: CellQueue::with_capacity(cap),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(crate) fn append_targets(&self, targets: &mut Vec<Token>) {
        self.operations
            .append_targets_with(targets, |token, hold| hold.targets(token));
    }

    pub(crate) fn begin(
        &self,
        path: OpenPath,
        flags: i32,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.operations
            .begin(OpenHold::direct(path), driver, |token, hold| {
                Some((token, unsafe { hold.path.open_at(flags, token) }))
            })
            .ok()
    }

    pub(crate) fn begin_fixed(
        &self,
        path: OpenPath,
        flags: i32,
        fd: &Fd<'_>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.operations
            .begin(OpenHold::fixed(path, fd.slot()), driver, |token, hold| {
                let flags = flags & !libc::O_CLOEXEC;
                unsafe {
                    Sqe::openat_fixed(
                        libc::AT_FDCWD,
                        hold.path.as_ptr(),
                        flags,
                        0,
                        fd.slot(),
                        token,
                    )
                }
                .ok()
                .map(|sqe| (token, sqe))
            })
            .ok()
    }

    pub(crate) fn poll(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<OpenDone> {
        match self.operations.poll(token, wake) {
            Some((_, done)) => FileOutcome::Done(done),
            None => FileOutcome::Pending,
        }
    }

    pub(crate) fn cancel(&self, token: Token) {
        let Some(hold) = self.operations.request_cancel(token) else {
            return;
        };
        if let OpenTarget::Fixed(slot) = hold.target {
            let _ = self.releases.push_back(slot);
        }
    }

    pub(crate) fn flush_cancellations(&self, driver: &mut DriverContext<'_, 'd>) -> bool {
        let quiesced = self
            .operations
            .flush_cancellations(driver, |token, hold| hold.targets(token));
        while let Some(slot) = self.releases.pop_front() {
            // SAFETY: the settled operation and its long handle were both
            // abandoned before this release queue became observable.
            drop(unsafe { driver.guard_raw(slot) });
        }
        quiesced
    }

    pub(crate) fn complete(&self, token: Token, e: OpenEvent, driver: &mut DriverContext<'_, 'd>) {
        self.operations
            .complete(token, e, driver, |hold, event| match event {
                OpenEvent::Opened(fd) => match hold.target {
                    OpenTarget::Direct => OpenDone::Direct(unsafe { OwnedFd::from_raw_fd(fd) }),
                    OpenTarget::Fixed(_) => OpenDone::Fixed,
                },
                OpenEvent::Failed(errno) => OpenDone::Failed(errno),
            });
    }
}
