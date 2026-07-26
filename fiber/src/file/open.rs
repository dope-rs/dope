use std::io;
use std::io::Error;
use std::pin::Pin;
use std::task::Poll;

use super::Source;
use super::already_done;
use crate::{Context, Fiber};
use dope::driver::token::Token;
use dope::io::file::OpenPath;
use dope::manifold::file::open::OpenDone;
use dope::manifold::file::{FileOutcome, Files};
use std::mem::replace;

enum OpenStage {
    Init(OpenPath),
    Pending(Token),
    Done,
}

pub struct Open<'h, 'd, const ID: u8, const N: usize> {
    host: &'h Files<'d, ID, N>,
    flags: i32,
    stage: OpenStage,
}

impl<'h, 'd, const ID: u8, const N: usize> Open<'h, 'd, ID, N> {
    pub fn direct(host: &'h Files<'d, ID, N>, path: OpenPath, flags: i32) -> Self {
        Self {
            host,
            flags,
            stage: OpenStage::Init(path),
        }
    }
}

impl<const ID: u8, const N: usize> Unpin for Open<'_, '_, ID, N> {}

impl<const ID: u8, const N: usize> Drop for Open<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let OpenStage::Pending(token) = self.stage {
            self.host.cancel_open(token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for Open<'h, 'd, ID, N> {
    type Output = io::Result<Source<'d>>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match replace(&mut this.stage, OpenStage::Done) {
            OpenStage::Done => return Poll::Ready(Err(already_done())),
            OpenStage::Pending(token) => token,
            OpenStage::Init(path) => {
                let mut driver = cx.as_mut().driver_access();
                let Some(token) = this.host.begin_open(path, this.flags, &mut driver) else {
                    return Poll::Ready(Err(Error::other("dope::file: open submit failed")));
                };
                token
            }
        };
        match this.host.poll_open(token, cx.completion_waker()) {
            FileOutcome::Done(OpenDone::Opened(fd)) => Poll::Ready(Ok(Source::owned(fd))),
            FileOutcome::Done(OpenDone::Failed(errno)) => {
                Poll::Ready(Err(Error::from_raw_os_error(errno)))
            }
            FileOutcome::Pending => {
                this.stage = OpenStage::Pending(token);
                Poll::Pending
            }
        }
    }
}
