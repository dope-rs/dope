use std::os::fd::OwnedFd;

use super::FileOutcome;
use super::raw::OpenRequest;
use super::raw::table::{CancellationSignal, OperationTable};
use dope::DriverContext;
use dope_core::driver::control::Quiesce;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::token::kind::OPEN;
use dope_core::driver::token::{KeyTag, Token, TokenCapacity};
use dope_core::io::OpenEvent;
use dope_core::io::file::OpenPath;

pub enum OpenDone {
    Opened(OwnedFd),
    Failed(i32),
}

pub(crate) struct OpenTable<'d, const ID: u8> {
    operations: OperationTable<'d, OpenRequest, OpenDone, KeyTag<ID, OPEN>>,
}

impl<'d, const ID: u8> OpenTable<'d, ID> {
    pub(crate) fn new(capacity: TokenCapacity) -> Self {
        Self {
            operations: OperationTable::with_capacity(capacity),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(crate) fn for_each_target(&self, visit: impl FnMut(Token)) {
        self.operations.for_each_target(visit);
    }

    pub(crate) fn begin(
        &self,
        path: OpenPath,
        flags: i32,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.operations
            .begin(OpenRequest::new(path), driver, |token, request| {
                Some((token, request.submission(flags, token)))
            })
            .ok()
    }

    pub(crate) fn poll(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<OpenDone> {
        match self.operations.poll(token, wake) {
            Some((_, done)) => FileOutcome::Done(done),
            None => FileOutcome::Pending,
        }
    }

    pub(super) fn cancel(&self, token: Token, signal: &CancellationSignal) {
        let _ = self.operations.request_cancel(token, signal);
    }

    pub(super) fn flush_cancellations(&self, quiesce: &mut Quiesce<'_>) {
        self.operations.flush_cancellations(quiesce);
    }

    pub(crate) fn complete(&self, token: Token, event: OpenEvent) {
        self.operations
            .complete(token, event, |_, event| match event {
                OpenEvent::Opened(fd) => OpenDone::Opened(fd),
                OpenEvent::Failed(errno) => OpenDone::Failed(errno),
            });
    }
}
