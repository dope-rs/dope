use std::io;
use std::mem;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use super::Source;
use super::already_done;
use crate::{Context, Fiber};
use dope::DriverContext;
use dope::driver::ready::CompletionWaker;
use dope::driver::token::Token;
use dope::manifold::file::{FileOutcome, Files, ReadDone};

enum Phase {
    Ready(Vec<u8>),
    Init {
        source: Rc<OwnedFd>,
        buffer: Vec<u8>,
    },
    Pending(Token),
    Done,
}

pub struct Read<'h, 'd, const ID: u8, const N: usize> {
    host: &'h Files<'d, ID, N>,
    offset: u64,
    phase: Phase,
}

impl<'h, 'd, const ID: u8, const N: usize> Read<'h, 'd, ID, N> {
    pub fn new(
        host: &'h Files<'d, ID, N>,
        source: &Source<'d>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> Self {
        let phase = if buffer.is_empty() {
            Phase::Ready(buffer)
        } else {
            Phase::Init {
                source: source.lease(),
                buffer,
            }
        };
        Self {
            host,
            offset,
            phase,
        }
    }

    fn begin(
        &self,
        source: Rc<OwnedFd>,
        buffer: Vec<u8>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Vec<u8>, io::Error)> {
        self.host.begin_read(source, buffer, self.offset, driver)
    }

    fn complete(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(Vec<u8>, ReadDone)> {
        self.host.poll_read(token, wake)
    }
}

impl<const ID: u8, const N: usize> Drop for Read<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let Phase::Pending(token) = self.phase {
            self.host.cancel_read(token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for Read<'h, 'd, ID, N> {
    type Output = (Vec<u8>, io::Result<usize>);

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match mem::replace(&mut this.phase, Phase::Done) {
            Phase::Done => return Poll::Ready((Vec::new(), Err(already_done()))),
            Phase::Ready(buffer) => return Poll::Ready((buffer, Ok(0))),
            Phase::Pending(token) => token,
            Phase::Init { source, buffer } => {
                let begun = {
                    let mut driver = cx.as_mut().driver_access();
                    this.begin(source, buffer, &mut driver)
                };
                match begun {
                    Ok(token) => token,
                    Err((buffer, error)) => return Poll::Ready((buffer, Err(error))),
                }
            }
        };
        match this.complete(token, cx.completion_waker()) {
            FileOutcome::Done((buffer, done)) => Poll::Ready((
                buffer,
                match done {
                    ReadDone::Complete(output) => Ok(output),
                    ReadDone::Failed(error) => Err(error),
                },
            )),
            FileOutcome::Pending => {
                this.phase = Phase::Pending(token);
                Poll::Pending
            }
        }
    }
}
