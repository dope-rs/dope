use std::io;
use std::io::{Error, ErrorKind};
use std::mem::replace;
use std::os::fd::AsRawFd;
use std::process::abort;

use dope::DriverContext;
use dope_core::backend::RawSqe;
use dope_core::driver::control::Quiesce;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::token::kind::READ;
use dope_core::driver::token::{KeyTag, Token, TokenCapacity};
use dope_core::io::ReadEvent;

use super::FileOutcome;
use super::raw::ReadRegion;
use super::raw::table::{CancellationSignal, OperationTable};
use super::source::Source;

pub enum ReadDone {
    Progress(u32),
    Eof,
    Failed(Error),
}

enum ReadFlight {
    Idle(Vec<u8>),
    Prepared(ReadRegion),
    Pending(ReadRegion),
}

struct ReadHold<'d> {
    source: Source<'d>,
    len: u32,
    offset: u64,
    flight: ReadFlight,
}

impl<'d> ReadHold<'d> {
    fn prepare_submission(&mut self, token: Token) -> io::Result<RawSqe> {
        if !matches!(self.flight, ReadFlight::Idle(_)) {
            abort();
        }
        let ReadFlight::Idle(buffer) = replace(&mut self.flight, ReadFlight::Idle(Vec::new()))
        else {
            abort();
        };
        let region = match ReadRegion::new(buffer, self.len) {
            Ok(region) => region,
            Err(buffer) => {
                self.flight = ReadFlight::Idle(buffer);
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "dope::file: read buffer has no writable region",
                ));
            }
        };
        let (sqe, region) = region.submission(self.source.as_raw_fd(), self.offset, token);
        self.flight = ReadFlight::Prepared(region);
        Ok(sqe)
    }

    fn accept_submission(&mut self) {
        let ReadFlight::Prepared(region) = replace(&mut self.flight, ReadFlight::Idle(Vec::new()))
        else {
            abort();
        };
        self.flight = ReadFlight::Pending(region);
    }

    fn abort_submission(&mut self) {
        let ReadFlight::Prepared(region) = replace(&mut self.flight, ReadFlight::Idle(Vec::new()))
        else {
            abort();
        };
        self.flight = ReadFlight::Idle(region.into_buffer());
    }

    fn finish(&mut self, amount: u32) -> io::Result<()> {
        let ReadFlight::Pending(region) = replace(&mut self.flight, ReadFlight::Idle(Vec::new()))
        else {
            abort();
        };
        match region.commit(amount) {
            Ok(buffer) => {
                self.flight = ReadFlight::Idle(buffer);
                Ok(())
            }
            Err((buffer, error)) => {
                self.flight = ReadFlight::Idle(buffer);
                Err(error)
            }
        }
    }

    fn into_parts(self) -> (Source<'d>, Vec<u8>) {
        let ReadFlight::Idle(buffer) = self.flight else {
            abort();
        };
        (self.source, buffer)
    }
}

pub(crate) struct ReadTable<'d, const ID: u8> {
    operations: OperationTable<'d, ReadHold<'d>, ReadDone, KeyTag<ID, READ>>,
}

impl<'d, const ID: u8> ReadTable<'d, ID> {
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
        source: Source<'d>,
        buffer: Vec<u8>,
        len: u32,
        offset: u64,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Source<'d>, Vec<u8>, Error)> {
        self.operations
            .begin_prepared(
                ReadHold {
                    source,
                    len,
                    offset,
                    flight: ReadFlight::Idle(buffer),
                },
                driver,
                |token, hold| Ok((token, hold.prepare_submission(token)?)),
                ReadHold::accept_submission,
                ReadHold::abort_submission,
            )
            .map_err(|(hold, error)| {
                let (source, buffer) = hold.into_parts();
                (source, buffer, error)
            })
    }

    pub(crate) fn poll(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(Source<'d>, Vec<u8>, ReadDone)> {
        match self.operations.poll(token, wake) {
            Some((hold, done)) => {
                let (source, buffer) = hold.into_parts();
                FileOutcome::Done((source, buffer, done))
            }
            None => FileOutcome::Pending,
        }
    }

    pub(super) fn cancel(&self, token: Token, signal: &CancellationSignal) {
        let _ = self.operations.request_cancel(token, signal);
    }

    pub(super) fn flush_cancellations(&self, quiesce: &mut Quiesce<'_>) {
        self.operations.flush_cancellations(quiesce);
    }

    pub(crate) fn complete(&self, token: Token, event: ReadEvent) {
        self.operations
            .complete(token, event, |hold, event| match event {
                ReadEvent::Read(amount) => match hold.finish(amount) {
                    Ok(()) => ReadDone::Progress(amount),
                    Err(error) => ReadDone::Failed(error),
                },
                ReadEvent::Eof => match hold.finish(0) {
                    Ok(()) => ReadDone::Eof,
                    Err(error) => ReadDone::Failed(error),
                },
                ReadEvent::Failed(errno) => match hold.finish(0) {
                    Ok(()) => ReadDone::Failed(Error::from_raw_os_error(errno)),
                    Err(error) => ReadDone::Failed(error),
                },
            });
    }
}
