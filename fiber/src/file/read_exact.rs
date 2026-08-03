use std::io;
use std::io::Error;
use std::mem::replace;
use std::pin::Pin;
use std::process::abort;
use std::task::Poll;

use dope::DriverContext;
use dope::driver::ready::CompletionWaker;
use dope::driver::token::Token;
use dope::manifold::file::read::ReadDone;
use dope::manifold::file::{FileOutcome, Files};

use super::Source;
use crate::raw::task::{CompletionOwner, CompletionRegistrar};
use crate::{Context, Fiber};

enum Phase<'d> {
    Ready { source: Source<'d>, buffer: Vec<u8> },
    Init { source: Source<'d>, buffer: Vec<u8> },
    Pending(Token),
    Done,
}

pub struct ReadExact<'h, 'd, const ID: u8, const N: usize> {
    host: &'h Files<'d, ID, N>,
    remaining: u32,
    offset: u64,
    phase: Phase<'d>,
}

// SAFETY: the owner records the token before retaining the handle, and its
// Drop cancels that exact registration on every pending exit.
unsafe impl<'h, 'd, const ID: u8, const N: usize> CompletionRegistrar<'d>
    for CompletionOwner<(&mut ReadExact<'h, 'd, ID, N>, Token)>
{
    type Output = Poll<(Source<'d>, Vec<u8>, ReadDone)>;

    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let (owner, token) = self.0;
        owner.phase = Phase::Pending(token);
        match owner.host.poll_read(token, wake) {
            FileOutcome::Done((source, buffer, done)) => {
                owner.phase = Phase::Done;
                Poll::Ready((source, buffer, done))
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> ReadExact<'h, 'd, ID, N> {
    pub fn new(host: &'h Files<'d, ID, N>, source: Source<'d>, len: u32, offset: u64) -> Self {
        let buffer = Vec::with_capacity(len as usize);
        let phase = if len == 0 {
            Phase::Ready { source, buffer }
        } else {
            Phase::Init { source, buffer }
        };
        Self {
            host,
            remaining: len,
            offset,
            phase,
        }
    }

    fn begin(
        &self,
        source: Source<'d>,
        buffer: Vec<u8>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Source<'d>, Vec<u8>, Error)> {
        self.host
            .begin_read(source, buffer, self.remaining, self.offset, driver)
    }
}

impl<const ID: u8, const N: usize> Drop for ReadExact<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let Phase::Pending(token) = self.phase {
            self.host.cancel_read(token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for ReadExact<'h, 'd, ID, N> {
    type Output = (Source<'d>, Vec<u8>, io::Result<()>);

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let token = match replace(&mut this.phase, Phase::Done) {
                Phase::Done => abort(),
                Phase::Ready { source, buffer } => {
                    return Poll::Ready((source, buffer, Ok(())));
                }
                Phase::Pending(token) => token,
                Phase::Init { source, buffer } => {
                    let begun = {
                        let mut driver = cx.as_mut().driver_access();
                        this.begin(source, buffer, &mut driver)
                    };
                    match begun {
                        Ok(token) => token,
                        Err((source, buffer, error)) => {
                            return Poll::Ready((source, buffer, Err(error)));
                        }
                    }
                }
            };
            let completed = cx
                .as_ref()
                .register_completion(CompletionOwner((&mut *this, token)));
            let Poll::Ready((source, buffer, done)) = completed else {
                return Poll::Pending;
            };
            match done {
                ReadDone::Progress(amount) if amount == this.remaining => {
                    return Poll::Ready((source, buffer, Ok(())));
                }
                ReadDone::Progress(amount) if amount < this.remaining => {
                    let Some(offset) = this.offset.checked_add(amount as u64) else {
                        return Poll::Ready((
                            source,
                            buffer,
                            Err(Error::new(
                                io::ErrorKind::InvalidInput,
                                "dope::file: read offset overflow",
                            )),
                        ));
                    };
                    this.remaining -= amount;
                    this.offset = offset;
                    this.phase = Phase::Init { source, buffer };
                }
                ReadDone::Progress(_) => abort(),
                ReadDone::Eof => {
                    return Poll::Ready((
                        source,
                        buffer,
                        Err(Error::from(io::ErrorKind::UnexpectedEof)),
                    ));
                }
                ReadDone::Failed(error) => {
                    return Poll::Ready((source, buffer, Err(error)));
                }
            }
        }
    }
}
