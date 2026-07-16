use std::io;
use std::io::Error;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use super::Stage;
use super::{Direct, Metadata, Source};
use crate::{Context, Fiber};
use dope::io::file::OpenPath;
use dope::manifold::file::{FileOutcome, Files, StatDone};

enum StatTarget {
    Fd(Rc<OwnedFd>),
    Path(OpenPath),
}

pub struct Stat<'h, 'd, const ID: u8, const N: usize> {
    host: &'h Files<'d, ID, N>,
    target: Option<StatTarget>,
    stage: Stage,
}

impl<'h, 'd, const ID: u8, const N: usize> Stat<'h, 'd, ID, N> {
    pub fn source(host: &'h Files<'d, ID, N>, source: &Source<'d, Direct>) -> Self {
        Self {
            host,
            target: Some(StatTarget::Fd(source.direct())),
            stage: Stage::Init,
        }
    }

    pub fn path(host: &'h Files<'d, ID, N>, path: OpenPath) -> Self {
        Self {
            host,
            target: Some(StatTarget::Path(path)),
            stage: Stage::Init,
        }
    }
}

impl<const ID: u8, const N: usize> Unpin for Stat<'_, '_, ID, N> {}

impl<const ID: u8, const N: usize> Drop for Stat<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let Stage::Pending(token) = self.stage {
            self.host.cancel_stat(token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for Stat<'h, 'd, ID, N> {
    type Output = io::Result<Metadata>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match this.stage {
            Stage::Done => return Poll::Ready(Err(Stage::already_done())),
            Stage::Pending(token) => token,
            Stage::Init => {
                let target = this.target.take().unwrap();
                let mut driver = cx.as_mut().driver_access();
                let token = match target {
                    StatTarget::Fd(fd) => this.host.begin_stat_fd(fd, &mut driver),
                    StatTarget::Path(path) => this.host.begin_stat_path(path, &mut driver),
                };
                let Some(token) = token else {
                    this.stage = Stage::Done;
                    return Poll::Ready(Err(Error::other("dope::file: stat submit failed")));
                };
                this.stage = Stage::Pending(token);
                token
            }
        };
        match this.host.poll_stat(token, cx.completion_waker()) {
            FileOutcome::Done(done) => {
                this.stage = Stage::Done;
                Poll::Ready(match done {
                    StatDone::Metadata(metadata) => Ok(metadata),
                    StatDone::Failed(errno) => Err(Error::from_raw_os_error(errno)),
                })
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
}
