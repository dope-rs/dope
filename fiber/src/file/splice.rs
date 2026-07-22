use std::io;
use std::io::Error;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use super::already_done;
use super::{Direct, Source};
use crate::{Context, Fiber};
use dope::manifold::file::{FileOutcome, Files, SpliceDone};

enum SpliceStage {
    Init {
        source: Rc<OwnedFd>,
        sink: Rc<OwnedFd>,
    },
    Pending(dope::driver::token::Token),
    Done,
}

pub struct SpliceToPipe<'h, 'd, const ID: u8, const N: usize> {
    host: &'h Files<'d, ID, N>,
    off_in: i64,
    len: u32,
    stage: SpliceStage,
}

impl<'h, 'd, const ID: u8, const N: usize> SpliceToPipe<'h, 'd, ID, N> {
    pub fn new(
        host: &'h Files<'d, ID, N>,
        source: &Source<'d, Direct>,
        sink: &Source<'d, Direct>,
        off_in: i64,
        len: u32,
    ) -> Self {
        Self {
            host,
            off_in,
            len,
            stage: SpliceStage::Init {
                source: source.direct(),
                sink: sink.direct(),
            },
        }
    }
}

impl<const ID: u8, const N: usize> Unpin for SpliceToPipe<'_, '_, ID, N> {}

impl<const ID: u8, const N: usize> Drop for SpliceToPipe<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let SpliceStage::Pending(token) = self.stage {
            self.host.cancel_splice(token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for SpliceToPipe<'h, 'd, ID, N> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match std::mem::replace(&mut this.stage, SpliceStage::Done) {
            SpliceStage::Done => return Poll::Ready(Err(already_done())),
            SpliceStage::Pending(token) => token,
            SpliceStage::Init { source, sink } => {
                let begun = {
                    let mut driver = cx.as_mut().driver_access();
                    this.host
                        .begin_splice_to_pipe(source, this.off_in, sink, this.len, &mut driver)
                };
                let Some(t) = begun else {
                    return Poll::Ready(Err(Error::other("dope::file: splice submit failed")));
                };
                t
            }
        };
        match this.host.poll_splice(token, cx.completion_waker()) {
            FileOutcome::Done(done) => Poll::Ready(match done {
                SpliceDone::Moved(n) => Ok(n as usize),
                SpliceDone::Eof => Ok(0),
                SpliceDone::Failed(errno) => Err(Error::from_raw_os_error(errno)),
            }),
            FileOutcome::Pending => {
                this.stage = SpliceStage::Pending(token);
                Poll::Pending
            }
        }
    }
}
