use std::io;
use std::os::fd::AsRawFd;

use o3::buffer::Block;

use super::FileOutcome;
use super::SourceRef;
use super::raw::ReadRegion;
use dope::DriverContext;
use dope_core::backend::Sqe;
use dope_core::driver::token::{KeyTag, Token};
use dope_core::io::ReadEvent;

use super::table::{CompletionAction, OperationTable, Targets};
use dope_core::driver::ready::CompletionWaker;

pub enum ReadDone {
    Complete(usize),
    Failed(io::Error),
}

pub(crate) trait ReadBuffer {
    fn prepare(&mut self) -> Option<ReadRegion>;

    fn wants_more(&self) -> bool;

    fn read_output(&self, amount: u32) -> usize;

    fn eof_output(&self) -> usize;

    fn commit(&mut self, region: ReadRegion, amount: u32) -> io::Result<()>;
}

impl ReadBuffer for Vec<u8> {
    fn prepare(&mut self) -> Option<ReadRegion> {
        ReadRegion::vec(self)
    }

    fn wants_more(&self) -> bool {
        false
    }

    fn read_output(&self, amount: u32) -> usize {
        amount as usize
    }

    fn eof_output(&self) -> usize {
        0
    }

    fn commit(&mut self, region: ReadRegion, amount: u32) -> io::Result<()> {
        region.commit_vec(self, amount)
    }
}

impl ReadBuffer for Block {
    fn prepare(&mut self) -> Option<ReadRegion> {
        ReadRegion::block(self)
    }

    fn wants_more(&self) -> bool {
        self.len() < Self::CAPACITY
    }

    fn read_output(&self, _amount: u32) -> usize {
        self.len()
    }

    fn eof_output(&self) -> usize {
        self.len()
    }

    fn commit(&mut self, region: ReadRegion, amount: u32) -> io::Result<()> {
        region.commit_block(self, amount)
    }
}

enum ReadFlight {
    Idle,
    Prepared(ReadRegion),
    Pending(ReadRegion),
}

struct ReadHold<'d, B> {
    buf: B,
    source: SourceRef<'d>,
    offset: u64,
    flight: ReadFlight,
}

impl<B: ReadBuffer> ReadHold<'_, B> {
    fn prepare_submission(&mut self, token: Token) -> io::Result<Sqe> {
        if !matches!(self.flight, ReadFlight::Idle) {
            return Err(Self::invalid_flight());
        }
        let region = self.buf.prepare().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope::file: read buffer has no writable region",
            )
        })?;
        let submission = match &self.source {
            SourceRef::Direct(fd) => region.direct(fd.as_raw_fd(), self.offset, token),
            SourceRef::Fixed(fd) => region.fixed(fd.slot(), self.offset, token),
        };
        let (sqe, region) = submission.into_parts();
        self.flight = ReadFlight::Prepared(region);
        Ok(sqe)
    }

    fn accept_submission(&mut self) {
        let flight = std::mem::replace(&mut self.flight, ReadFlight::Idle);
        let ReadFlight::Prepared(region) = flight else {
            std::process::abort();
        };
        self.flight = ReadFlight::Pending(region);
    }

    fn abort_submission(&mut self) {
        let flight = std::mem::replace(&mut self.flight, ReadFlight::Idle);
        if !matches!(flight, ReadFlight::Prepared(_)) {
            std::process::abort();
        }
    }

    fn commit_read(&mut self, amount: u32) -> io::Result<()> {
        let flight = std::mem::replace(&mut self.flight, ReadFlight::Idle);
        let ReadFlight::Pending(region) = flight else {
            return Err(Self::invalid_flight());
        };
        self.buf.commit(region, amount)
    }

    fn finish_without_data(&mut self) -> io::Result<()> {
        let flight = std::mem::replace(&mut self.flight, ReadFlight::Idle);
        if matches!(flight, ReadFlight::Pending(_)) {
            Ok(())
        } else {
            Err(Self::invalid_flight())
        }
    }

    fn invalid_flight() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "dope::file: read completion does not match a pending region",
        )
    }
}

pub(crate) struct ReadTable<'d, B, const ID: u8, const KIND: u8> {
    operations: OperationTable<'d, ReadHold<'d, B>, ReadDone, KeyTag<ID, KIND>>,
}

impl<'d, B: ReadBuffer, const ID: u8, const KIND: u8> ReadTable<'d, B, ID, KIND> {
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
        source: SourceRef<'d>,
        buf: B,
        offset: u64,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (B, io::Error)> {
        self.operations
            .begin_prepared(
                ReadHold {
                    buf,
                    source,
                    offset,
                    flight: ReadFlight::Idle,
                },
                driver,
                |token, held| Ok((token, held.prepare_submission(token)?)),
                ReadHold::accept_submission,
                ReadHold::abort_submission,
            )
            .map_err(|(hold, error)| (hold.buf, error))
    }

    pub(crate) fn poll(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(B, ReadDone)> {
        match self.operations.poll(token, wake) {
            Some((hold, done)) => FileOutcome::Done((hold.buf, done)),
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
        event: ReadEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.operations
            .complete(token, event, driver, |hold, event| match event {
                ReadEvent::Read(amount) => {
                    if let Err(error) = hold.commit_read(amount) {
                        return CompletionAction::Settle(ReadDone::Failed(error));
                    }
                    if !hold.buf.wants_more() {
                        let output = hold.buf.read_output(amount);
                        return CompletionAction::Settle(ReadDone::Complete(output));
                    }
                    let Some(offset) = hold.offset.checked_add(amount as u64) else {
                        return CompletionAction::Settle(ReadDone::Failed(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "dope::file: read offset overflow",
                        )));
                    };
                    hold.offset = offset;
                    match hold.prepare_submission(token) {
                        Ok(sqe) => CompletionAction::Resubmit {
                            sqe,
                            accepted: ReadHold::accept_submission,
                            aborted: ReadHold::abort_submission,
                            failed: |error| ReadDone::Failed(error.into()),
                        },
                        Err(error) => CompletionAction::Settle(ReadDone::Failed(error)),
                    }
                }
                ReadEvent::Eof => match hold.finish_without_data() {
                    Ok(()) => CompletionAction::Settle(ReadDone::Complete(hold.buf.eof_output())),
                    Err(error) => CompletionAction::Settle(ReadDone::Failed(error)),
                },
                ReadEvent::Failed(errno) => match hold.finish_without_data() {
                    Ok(()) => CompletionAction::Settle(ReadDone::Failed(
                        io::Error::from_raw_os_error(errno),
                    )),
                    Err(error) => CompletionAction::Settle(ReadDone::Failed(error)),
                },
            });
    }
}
