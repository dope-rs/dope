use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use super::FileOutcome;
use super::Metadata;
use dope::DriverContext;
use dope_core::backend::{Backend, Sqe};
use dope_core::driver::token::kind::STAT;
use dope_core::driver::token::{KeyTag, Token};
use dope_core::io::StatEvent;
use dope_core::io::file::OpenPath;
use dope_core::platform::Platform;

type StatBuf = <Backend as Platform>::StatBuf;

use super::table::OperationTable;
use dope_core::driver::ready::CompletionWaker;

pub enum StatDone {
    Metadata(Metadata),
    Failed(io::Error),
}

enum StatSource {
    Path(OpenPath),
    Fd(Rc<OwnedFd>),
}

struct StatHold {
    source: StatSource,
    stat: MaybeUninit<StatBuf>,
}

pub(crate) struct StatTable<'d, const ID: u8> {
    operations: OperationTable<'d, StatHold, StatDone, KeyTag<ID, STAT>>,
}

impl<'d, const ID: u8> StatTable<'d, ID> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            operations: OperationTable::with_capacity(cap),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(crate) fn append_targets(&self, targets: &mut Vec<Token>) {
        self.operations.append_targets(targets);
    }

    pub(crate) fn begin_path(
        &self,
        path: OpenPath,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.begin(StatSource::Path(path), driver)
    }

    pub(crate) fn begin_fd(
        &self,
        fd: Rc<OwnedFd>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.begin(StatSource::Fd(fd), driver)
    }

    fn begin(&self, source: StatSource, driver: &mut DriverContext<'_, 'd>) -> Option<Token> {
        self.operations
            .begin(
                StatHold {
                    source,
                    stat: MaybeUninit::zeroed(),
                },
                driver,
                |token, hold| {
                    let stat = hold.stat.as_mut_ptr();
                    let sqe = match &hold.source {
                        StatSource::Path(path) => Sqe::stat_path(path.as_ptr(), stat, token),
                        StatSource::Fd(fd) => Sqe::stat_fd(fd.as_raw_fd(), stat, token),
                    };
                    Some((token, sqe))
                },
            )
            .ok()
    }

    pub(crate) fn poll(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<StatDone> {
        match self.operations.poll(token, wake) {
            Some((_, done)) => FileOutcome::Done(done),
            None => FileOutcome::Pending,
        }
    }

    pub(crate) fn cancel(&self, token: Token) {
        let _ = self.operations.request_cancel(token);
    }

    pub(crate) fn flush_cancellations(&self, driver: &mut DriverContext<'_, 'd>) -> bool {
        self.operations.flush_cancellations(driver)
    }

    pub(crate) fn complete(&self, token: Token, event: StatEvent) {
        self.operations
            .complete(token, event, |hold, event| match event {
                StatEvent::Done => {
                    let raw = unsafe { hold.stat.assume_init_read() };
                    match Backend::parse_meta(&raw) {
                        Ok(metadata) => StatDone::Metadata(Metadata::from_raw(metadata)),
                        Err(error) => StatDone::Failed(error),
                    }
                }
                StatEvent::Failed(errno) => StatDone::Failed(io::Error::from_raw_os_error(errno)),
            });
    }
}
