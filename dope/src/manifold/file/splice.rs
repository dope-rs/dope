use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use super::FileOutcome;
use dope::DriverContext;
use dope_core::backend::Sqe;
use dope_core::driver::token::kind::SPLICE;
use dope_core::driver::token::{KeyTag, Token};
use dope_core::io::SpliceEvent;

use super::table::{OperationTable, Targets};
use dope_core::driver::ready::CompletionWaker;

#[derive(Clone, Copy)]
pub enum SpliceDone {
    Moved(u32),
    Eof,
    Failed(i32),
}

struct SpliceHold {
    source: Rc<OwnedFd>,
    sink: Rc<OwnedFd>,
}

pub(crate) struct SpliceTable<'d, const ID: u8> {
    operations: OperationTable<'d, SpliceHold, SpliceDone, KeyTag<ID, SPLICE>>,
}

impl<'d, const ID: u8> SpliceTable<'d, ID> {
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

    pub(crate) fn begin(
        &self,
        source: Rc<OwnedFd>,
        off_in: i64,
        sink: Rc<OwnedFd>,
        len: u32,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.operations
            .begin(SpliceHold { source, sink }, driver, |token, hold| {
                Some((
                    token,
                    Sqe::splice_to_pipe(
                        hold.source.as_raw_fd(),
                        off_in,
                        hold.sink.as_raw_fd(),
                        len,
                        token,
                    ),
                ))
            })
            .ok()
    }

    pub(crate) fn poll(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<SpliceDone> {
        match self.operations.poll(token, wake) {
            Some((_, done)) => FileOutcome::Done(done),
            None => FileOutcome::Pending,
        }
    }

    pub(crate) fn cancel(&self, token: Token) {
        let _ = self.operations.request_cancel(token);
    }

    pub(crate) fn flush_cancellations(&self, driver: &mut DriverContext<'_, 'd>) -> bool {
        self.operations
            .flush_cancellations(driver, |token, _| Targets::one(token))
    }

    pub(crate) fn complete(
        &self,
        token: Token,
        e: SpliceEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.operations
            .complete(token, e, driver, |_, event| match event {
                SpliceEvent::Moved(n) => SpliceDone::Moved(n),
                SpliceEvent::Eof => SpliceDone::Eof,
                SpliceEvent::Failed(errno) => SpliceDone::Failed(errno),
            });
    }
}
