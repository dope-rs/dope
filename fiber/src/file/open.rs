use std::io;
use std::io::Error;
use std::mem::replace;
use std::pin::Pin;
use std::task::Poll;

use dope::driver::ready::CompletionWaker;
use dope::driver::token::Token;
use dope::io::file::OpenPath;
use dope::manifold::file::open::OpenDone;
use dope::manifold::file::{FileOutcome, Files};

use super::{Source, already_done};
use crate::raw::task::{CompletionOwner, CompletionRegistrar};
use crate::{Context, Fiber};

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

// SAFETY: the owner records the token before retaining the handle, and its
// Drop cancels that exact registration on every pending exit.
unsafe impl<'h, 'd, const ID: u8, const N: usize> CompletionRegistrar<'d>
    for CompletionOwner<(&mut Open<'h, 'd, ID, N>, Token)>
{
    type Output = Poll<io::Result<Source<'d>>>;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let (owner, token) = self.0;
        owner.stage = OpenStage::Pending(token);
        match owner.host.poll_open(token, wake) {
            FileOutcome::Done(OpenDone::Opened(fd)) => {
                owner.stage = OpenStage::Done;
                Poll::Ready(Ok(Source::owned(fd)))
            }
            FileOutcome::Done(OpenDone::Failed(errno)) => {
                owner.stage = OpenStage::Done;
                Poll::Ready(Err(Error::from_raw_os_error(errno)))
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
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
        cx.as_ref()
            .register_completion(CompletionOwner((this, token)))
    }
}
