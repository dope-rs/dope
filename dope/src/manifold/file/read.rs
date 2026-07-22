use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use super::FileOutcome;
use super::raw::ReadRegion;
use super::table::OperationTable;
use dope::DriverContext;
use dope_core::backend::Sqe;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::token::kind::READ;
use dope_core::driver::token::{KeyTag, Token};
use dope_core::io::ReadEvent;

pub enum ReadDone {
    Complete(usize),
    Failed(io::Error),
}

enum ReadFlight {
    Idle,
    Prepared(ReadRegion),
    Pending(ReadRegion),
}

struct ReadHold {
    buffer: Vec<u8>,
    source: Rc<OwnedFd>,
    offset: u64,
    flight: ReadFlight,
}

impl ReadHold {
    fn prepare_submission(&mut self, token: Token) -> io::Result<Sqe> {
        if !matches!(self.flight, ReadFlight::Idle) {
            return Err(Self::invalid_flight());
        }
        let region = ReadRegion::new(&mut self.buffer).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope::file: read buffer has no writable region",
            )
        })?;
        let (sqe, region) = region.submission(self.source.as_raw_fd(), self.offset, token);
        self.flight = ReadFlight::Prepared(region);
        Ok(sqe)
    }

    fn accept_submission(&mut self) {
        let ReadFlight::Prepared(region) = std::mem::replace(&mut self.flight, ReadFlight::Idle)
        else {
            std::process::abort();
        };
        self.flight = ReadFlight::Pending(region);
    }

    fn abort_submission(&mut self) {
        if !matches!(
            std::mem::replace(&mut self.flight, ReadFlight::Idle),
            ReadFlight::Prepared(_)
        ) {
            std::process::abort();
        }
    }

    fn finish(&mut self, amount: u32) -> io::Result<()> {
        let ReadFlight::Pending(region) = std::mem::replace(&mut self.flight, ReadFlight::Idle)
        else {
            return Err(Self::invalid_flight());
        };
        region.commit(&mut self.buffer, amount)
    }

    fn invalid_flight() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "dope::file: read completion does not match a pending region",
        )
    }
}

pub(crate) struct ReadTable<'d, const ID: u8> {
    operations: OperationTable<'d, ReadHold, ReadDone, KeyTag<ID, READ>>,
}

impl<'d, const ID: u8> ReadTable<'d, ID> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            operations: OperationTable::with_capacity(capacity),
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
        buffer: Vec<u8>,
        offset: u64,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Vec<u8>, io::Error)> {
        self.operations
            .begin_prepared(
                ReadHold {
                    buffer,
                    source,
                    offset,
                    flight: ReadFlight::Idle,
                },
                driver,
                |token, hold| Ok((token, hold.prepare_submission(token)?)),
                ReadHold::accept_submission,
                ReadHold::abort_submission,
            )
            .map_err(|(hold, error)| (hold.buffer, error))
    }

    pub(crate) fn poll(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(Vec<u8>, ReadDone)> {
        match self.operations.poll(token, wake) {
            Some((hold, done)) => FileOutcome::Done((hold.buffer, done)),
            None => FileOutcome::Pending,
        }
    }

    pub(crate) fn cancel(&self, token: Token) {
        let _ = self.operations.request_cancel(token);
    }

    pub(crate) fn flush_cancellations(&self, driver: &mut DriverContext<'_, 'd>) -> bool {
        self.operations.flush_cancellations(driver)
    }

    pub(crate) fn complete(&self, token: Token, event: ReadEvent) {
        self.operations
            .complete(token, event, |hold, event| match event {
                ReadEvent::Read(amount) => match hold.finish(amount) {
                    Ok(()) => ReadDone::Complete(amount as usize),
                    Err(error) => ReadDone::Failed(error),
                },
                ReadEvent::Eof => match hold.finish(0) {
                    Ok(()) => ReadDone::Complete(0),
                    Err(error) => ReadDone::Failed(error),
                },
                ReadEvent::Failed(errno) => match hold.finish(0) {
                    Ok(()) => ReadDone::Failed(io::Error::from_raw_os_error(errno)),
                    Err(error) => ReadDone::Failed(error),
                },
            });
    }
}
