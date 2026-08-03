use std::io;
use std::io::Error;
use std::mem::replace;
use std::pin::Pin;
use std::process::abort;
use std::task::Poll;

use dope::driver::ready::CompletionWaker;
use dope::driver::token::Token;
use dope::io::file::OpenPath;
use dope::manifold::file::stat::StatDone;
use dope::manifold::file::{FileOutcome, Files};

use super::{Metadata, Source, already_done};
use crate::raw::task::{CompletionOwner, CompletionRegistrar};
use crate::{Context, Fiber};

enum StatStage<T> {
    Init(T),
    Pending(Token),
    Done,
}

pub struct Stat<'h, 'd, const ID: u8, const N: usize, T = OpenPath> {
    host: &'h Files<'d, ID, N>,
    stage: StatStage<T>,
}

fn outcome(done: StatDone) -> io::Result<Metadata> {
    match done {
        StatDone::Metadata(metadata) => Ok(metadata),
        StatDone::Failed(error) => Err(error),
    }
}

// SAFETY: the owner records the token before retaining the handle, and its
// Drop cancels that exact registration on every pending exit.
unsafe impl<'h, 'd, const ID: u8, const N: usize> CompletionRegistrar<'d>
    for CompletionOwner<(&mut Stat<'h, 'd, ID, N, OpenPath>, Token)>
{
    type Output = Poll<io::Result<Metadata>>;

    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let (owner, token) = self.0;
        owner.stage = StatStage::Pending(token);
        match owner.host.poll_stat_path(token, wake) {
            FileOutcome::Done(done) => {
                owner.stage = StatStage::Done;
                Poll::Ready(outcome(done))
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
}

// SAFETY: the owner records the token before retaining the handle, and its
// Drop cancels that exact registration on every pending exit.
unsafe impl<'h, 'd, const ID: u8, const N: usize> CompletionRegistrar<'d>
    for CompletionOwner<(&mut Stat<'h, 'd, ID, N, Source<'d>>, Token)>
{
    type Output = Poll<(Source<'d>, io::Result<Metadata>)>;

    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let (owner, token) = self.0;
        owner.stage = StatStage::Pending(token);
        match owner.host.poll_stat_fd(token, wake) {
            FileOutcome::Done((source, done)) => {
                owner.stage = StatStage::Done;
                Poll::Ready((source, outcome(done)))
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Stat<'h, 'd, ID, N, OpenPath> {
    pub fn path(host: &'h Files<'d, ID, N>, path: OpenPath) -> Self {
        Self {
            host,
            stage: StatStage::Init(path),
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Stat<'h, 'd, ID, N, Source<'d>> {
    pub fn source(host: &'h Files<'d, ID, N>, source: Source<'d>) -> Self {
        Self {
            host,
            stage: StatStage::Init(source),
        }
    }
}

impl<T, const ID: u8, const N: usize> Unpin for Stat<'_, '_, ID, N, T> {}

impl<T, const ID: u8, const N: usize> Drop for Stat<'_, '_, ID, N, T> {
    fn drop(&mut self) {
        if let StatStage::Pending(token) = self.stage {
            self.host.cancel_stat(token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for Stat<'h, 'd, ID, N, OpenPath> {
    type Output = io::Result<Metadata>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match replace(&mut this.stage, StatStage::Done) {
            StatStage::Done => return Poll::Ready(Err(already_done())),
            StatStage::Pending(token) => token,
            StatStage::Init(path) => {
                let begun = {
                    let mut driver = cx.as_mut().driver_access();
                    this.host.begin_stat_path(path, &mut driver)
                };
                let Ok(token) = begun else {
                    return Poll::Ready(Err(Error::other("dope::file: stat submit failed")));
                };
                token
            }
        };
        cx.as_ref()
            .register_completion(CompletionOwner((this, token)))
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for Stat<'h, 'd, ID, N, Source<'d>> {
    type Output = (Source<'d>, io::Result<Metadata>);

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match replace(&mut this.stage, StatStage::Done) {
            StatStage::Done => abort(),
            StatStage::Pending(token) => token,
            StatStage::Init(source) => {
                let begun = {
                    let mut driver = cx.as_mut().driver_access();
                    this.host.begin_stat_fd(source, &mut driver)
                };
                match begun {
                    Ok(token) => token,
                    Err(source) => {
                        return Poll::Ready((
                            source,
                            Err(Error::other("dope::file: stat submit failed")),
                        ));
                    }
                }
            }
        };
        cx.as_ref()
            .register_completion(CompletionOwner((this, token)))
    }
}
