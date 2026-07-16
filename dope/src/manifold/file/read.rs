use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;

use o3::buffer::Block;

use super::FileOutcome;
use super::SourceRef;
use dope::DriverContext;
use dope_core::backend::Sqe;
use dope_core::driver::token::{KeyTag, Token};
use dope_core::io::ReadEvent;

use super::table::{CompletionAction, OperationTable, Targets};
use dope_core::driver::ready::CompletionWaker;

#[derive(Clone, Copy)]
pub enum ReadDone {
    Complete(usize),
    Failed(i32),
    OffsetOverflow,
    SubmitFailed,
}

pub(crate) trait ReadBuffer {
    fn spare(&mut self) -> &mut [MaybeUninit<u8>];

    fn wants_more(&self) -> bool;

    fn read_output(&self, amount: u32) -> usize;

    fn eof_output(&self) -> usize;

    unsafe fn advance(&mut self, amount: usize);
}

impl ReadBuffer for Vec<u8> {
    fn spare(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr().cast(), self.len()) }
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

    unsafe fn advance(&mut self, _amount: usize) {}
}

impl ReadBuffer for Block {
    fn spare(&mut self) -> &mut [MaybeUninit<u8>] {
        let mut writer = self.spare_writer();
        let ptr = writer.as_mut_ptr().cast();
        let capacity = writer.remaining();
        drop(writer);
        unsafe { std::slice::from_raw_parts_mut(ptr, capacity) }
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

    unsafe fn advance(&mut self, amount: usize) {
        let mut writer = self.spare_writer();
        assert!(amount <= writer.remaining(), "file read overflow");
        let initialized = unsafe { std::slice::from_raw_parts(writer.as_mut_ptr(), amount) };
        writer.try_commit_initialized(initialized).unwrap();
    }
}

struct ReadHold<'d, B> {
    buf: B,
    source: SourceRef<'d>,
    offset: u64,
    submitted: usize,
}

impl<B: ReadBuffer> ReadHold<'_, B> {
    fn submission(&mut self, token: Token) -> Option<Sqe> {
        let buf = self.buf.spare();
        if buf.is_empty() {
            return None;
        }
        self.submitted = buf.len();
        Some(match &self.source {
            SourceRef::Direct(fd) => unsafe {
                Sqe::read_uninit(fd.as_raw_fd(), buf, self.offset, token)
            },
            SourceRef::Fixed(fd) => Sqe::read_fixed_file_uninit(fd.slot(), buf, self.offset, token),
        })
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
    ) -> Result<Token, B> {
        self.operations
            .begin(
                ReadHold {
                    buf,
                    source,
                    offset,
                    submitted: 0,
                },
                driver,
                |token, held| held.submission(token).map(|sqe| (token, sqe)),
            )
            .map_err(|hold| hold.buf)
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
                    assert!(amount as usize <= hold.submitted, "file read overflow");
                    unsafe { hold.buf.advance(amount as usize) };
                    if !hold.buf.wants_more() {
                        let output = hold.buf.read_output(amount);
                        return CompletionAction::Settle(ReadDone::Complete(output));
                    }
                    let Some(offset) = hold.offset.checked_add(amount as u64) else {
                        return CompletionAction::Settle(ReadDone::OffsetOverflow);
                    };
                    hold.offset = offset;
                    match hold.submission(token) {
                        Some(sqe) => CompletionAction::Resubmit {
                            sqe,
                            failed: ReadDone::SubmitFailed,
                        },
                        None => CompletionAction::Settle(ReadDone::SubmitFailed),
                    }
                }
                ReadEvent::Eof => {
                    CompletionAction::Settle(ReadDone::Complete(hold.buf.eof_output()))
                }
                ReadEvent::Failed(errno) => CompletionAction::Settle(ReadDone::Failed(errno)),
            });
    }
}
