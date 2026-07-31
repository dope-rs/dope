use super::FileOutcome;
use super::metadata::Metadata;
use super::raw::StatRequest;
use super::source::Source;
use dope::DriverContext;
use dope_core::backend::Backend;
use dope_core::driver::token::kind::STAT;
use dope_core::driver::token::{KeyTag, Token, TokenCapacity};
use dope_core::io::StatEvent;
use dope_core::io::file::OpenPath;
use dope_core::platform::Platform;

use super::raw::table::{CancellationSignal, OperationTable};
use dope_core::driver::control::Quiesce;
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
    pub(crate) fn new(cap: TokenCapacity) -> Self {
        Self {
            operations: OperationTable::with_capacity(cap),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(crate) fn for_each_target(&self, visit: impl FnMut(Token)) {
        self.operations.for_each_target(visit);
    }

    pub(crate) fn begin_path(
        &self,
        path: OpenPath,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, OpenPath> {
        self.begin(StatRequest::path(path), driver)
            .map_err(StatRequest::into_path)
    }

    pub(crate) fn begin_fd(
        &self,
        source: Source<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, Source<'d>> {
        self.begin(StatRequest::fd(source), driver)
            .map_err(StatRequest::into_source)
    }

    fn begin(
        &self,
        request: StatRequest<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, StatRequest<'d>> {
        self.operations.begin(request, driver, |token, request| {
            Some((token, request.submission(token)))
        })
    }

    pub(crate) fn poll_path(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<StatDone> {
        match self.operations.poll(token, wake) {
            Some((request, done)) => {
                drop(request.into_path());
                FileOutcome::Done(done)
            }
            None => FileOutcome::Pending,
        }
    }

    pub(crate) fn poll_fd(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(Source<'d>, StatDone)> {
        match self.operations.poll(token, wake) {
            Some((request, done)) => FileOutcome::Done((request.into_source(), done)),
            None => FileOutcome::Pending,
        }
    }

    pub(super) fn cancel(&self, token: Token, signal: &CancellationSignal) {
        let _ = self.operations.request_cancel(token, signal);
    }

    pub(super) fn flush_cancellations(&self, quiesce: &mut Quiesce<'_>) {
        self.operations.flush_cancellations(quiesce);
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
