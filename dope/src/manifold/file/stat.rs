use super::FileOutcome;
use super::metadata::Metadata;
use super::raw::StatRequest;
use super::source::Source;
use dope::DriverContext;
use dope_core::backend::Backend;
use dope_core::driver::token::kind::STAT;
use dope_core::driver::token::{KeyTag, Token};
use dope_core::io::StatEvent;
use dope_core::io::file::OpenPath;
use dope_core::platform::Platform;

use super::raw::table::OperationTable;
use dope_core::driver::ready::CompletionWaker;
use std::io::Error;

pub enum StatDone {
    Metadata(Metadata),
    Failed(Error),
}

pub(crate) struct StatTable<'d, const ID: u8> {
    operations: OperationTable<'d, StatRequest<'d>, StatDone, KeyTag<ID, STAT>>,
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
        self.begin(StatRequest::path(path), driver)
    }

    pub(crate) fn begin_fd(
        &self,
        source: Source<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.begin(StatRequest::fd(source), driver)
    }

    fn begin(&self, request: StatRequest<'d>, driver: &mut DriverContext<'_, 'd>) -> Option<Token> {
        self.operations
            .begin(request, driver, |token, request| {
                Some((token, request.submission(token)))
            })
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
            .complete(token, event, |request, event| match event {
                StatEvent::Done => {
                    let raw = request.complete();
                    match Backend::parse_meta(&raw) {
                        Ok(metadata) => StatDone::Metadata(Metadata::from_raw(metadata)),
                        Err(error) => StatDone::Failed(error),
                    }
                }
                StatEvent::Failed(errno) => StatDone::Failed(Error::from_raw_os_error(errno)),
            });
    }
}
