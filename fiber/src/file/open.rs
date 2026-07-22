use std::io;
use std::io::Error;
use std::mem;
use std::pin::Pin;
use std::task::Poll;

use super::already_done;
use super::{Direct, Fixed, Source};
use crate::{Context, Fiber};
use dope::driver::token::Token;
use dope::io::file::OpenPath;
use dope::manifold::file::{FileOutcome, Files, OpenDone};
use dope_core::io::fd::Fd;

pub trait OpenKind: Sized {
    type Slot<'d>;

    fn begin<'d, const ID: u8, const N: usize>(
        host: &Files<'d, ID, N>,
        path: OpenPath,
        flags: i32,
        slot: &Self::Slot<'d>,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Option<Token>;

    fn source<'d>(slot: Self::Slot<'d>, done: OpenDone) -> Source<'d, Self>;
}

impl OpenKind for Direct {
    type Slot<'d> = ();

    fn begin<'d, const ID: u8, const N: usize>(
        host: &Files<'d, ID, N>,
        path: OpenPath,
        flags: i32,
        _slot: &Self::Slot<'d>,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Option<Token> {
        host.begin_open(path, flags, driver)
    }

    fn source<'d>(_slot: Self::Slot<'d>, done: OpenDone) -> Source<'d, Self> {
        match done {
            OpenDone::Direct(fd) => Source::owned(fd),
            _ => unreachable!(),
        }
    }
}

impl OpenKind for Fixed {
    type Slot<'d> = Fd<'d>;

    fn begin<'d, const ID: u8, const N: usize>(
        host: &Files<'d, ID, N>,
        path: OpenPath,
        flags: i32,
        slot: &Self::Slot<'d>,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Option<Token> {
        host.begin_open_fixed(path, flags, slot, driver)
    }

    fn source<'d>(slot: Self::Slot<'d>, done: OpenDone) -> Source<'d, Self> {
        match done {
            OpenDone::Fixed => Source::fixed(slot),
            _ => unreachable!(),
        }
    }
}

enum OpenStage<S> {
    Init { path: OpenPath, slot: S },
    Pending { token: Token, slot: S },
    Done,
}

pub struct Open<'h, 'd, const ID: u8, const N: usize, K: OpenKind = Direct> {
    host: &'h Files<'d, ID, N>,
    flags: i32,
    stage: OpenStage<K::Slot<'d>>,
}

impl<'h, 'd, const ID: u8, const N: usize> Open<'h, 'd, ID, N, Direct> {
    pub fn direct(host: &'h Files<'d, ID, N>, path: OpenPath, flags: i32) -> Self {
        Self {
            host,
            flags,
            stage: OpenStage::Init { path, slot: () },
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Open<'h, 'd, ID, N, Fixed> {
    pub fn fixed(host: &'h Files<'d, ID, N>, path: OpenPath, flags: i32, fixed: Fd<'d>) -> Self {
        Self {
            host,
            flags,
            stage: OpenStage::Init { path, slot: fixed },
        }
    }
}

impl<const ID: u8, const N: usize, K: OpenKind> Unpin for Open<'_, '_, ID, N, K> {}

impl<const ID: u8, const N: usize, K: OpenKind> Drop for Open<'_, '_, ID, N, K> {
    fn drop(&mut self) {
        if let OpenStage::Pending { token, .. } = &self.stage {
            self.host.cancel_open(*token);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize, K: OpenKind> Fiber<'d> for Open<'h, 'd, ID, N, K> {
    type Output = io::Result<Source<'d, K>>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (token, slot) = match mem::replace(&mut this.stage, OpenStage::Done) {
            OpenStage::Done => return Poll::Ready(Err(already_done())),
            OpenStage::Pending { token, slot } => (token, slot),
            OpenStage::Init { path, slot } => {
                let begun = {
                    let mut driver = cx.as_mut().driver_access();
                    K::begin(this.host, path, this.flags, &slot, &mut driver)
                };
                let Some(t) = begun else {
                    return Poll::Ready(Err(Error::other("dope::file: open submit failed")));
                };
                (t, slot)
            }
        };
        match this.host.poll_open(token, cx.completion_waker()) {
            FileOutcome::Done(done) => Poll::Ready(match done {
                OpenDone::Failed(errno) => Err(Error::from_raw_os_error(errno)),
                done => Ok(K::source(slot, done)),
            }),
            FileOutcome::Pending => {
                this.stage = OpenStage::Pending { token, slot };
                Poll::Pending
            }
        }
    }
}
