use std::io;
use std::io::Error;
use std::pin::Pin;
use std::task::Poll;

use super::already_done;
use super::{Metadata, Source};
use crate::{Context, Fiber};
use dope::driver::token::Token;
use dope::io::file::OpenPath;
use dope::manifold::file::stat::StatDone;
use dope::manifold::file::{FileOutcome, Files};
use std::mem::replace;

enum StatTarget<'d> {
    Fd(Source<'d>),
    Path(OpenPath),
}

enum StatStage<'d> {
    Init(StatTarget<'d>),
    Pending(Token),
    Done,
}

pub struct Stat<'h, 'd, const ID: u8, const N: usize> {
    host: &'h Files<'d, ID, N>,
    stage: StatStage<'d>,
}

impl<'h, 'd, const ID: u8, const N: usize> Stat<'h, 'd, ID, N> {
    pub fn source(host: &'h Files<'d, ID, N>, source: &Source<'d>) -> Self {
        Self {
            host,
            stage: StatStage::Init(StatTarget::Fd(source.clone())),
        }
    }

    pub fn path(host: &'h Files<'d, ID, N>, path: OpenPath) -> Self {
        Self {
            host,
            stage: StatStage::Init(StatTarget::Path(path)),
        }
    }
}

impl<const ID: u8, const N: usize> Unpin for Stat<'_, '_, ID, N> {}

impl<const ID: u8, const N: usize> Drop for Stat<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let StatStage::Pending(token) = self.stage {
            self.host.cancel_stat(token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for Stat<'h, 'd, ID, N> {
    type Output = io::Result<Metadata>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match replace(&mut this.stage, StatStage::Done) {
            StatStage::Done => return Poll::Ready(Err(already_done())),
            StatStage::Pending(token) => token,
            StatStage::Init(target) => {
                let mut driver = cx.as_mut().driver_access();
                let token = match target {
                    StatTarget::Fd(fd) => this.host.begin_stat_fd(fd, &mut driver),
                    StatTarget::Path(path) => this.host.begin_stat_path(path, &mut driver),
                };
                let Some(token) = token else {
                    return Poll::Ready(Err(Error::other("dope::file: stat submit failed")));
                };
                token
            }
        };
        match this.host.poll_stat(token, cx.completion_waker()) {
            FileOutcome::Done(done) => Poll::Ready(match done {
                StatDone::Metadata(metadata) => Ok(metadata),
                StatDone::Failed(error) => Err(error),
            }),
            FileOutcome::Pending => {
                this.stage = StatStage::Pending(token);
                Poll::Pending
            }
        }
    }
}
