use std::{io, task};

use dope::{
    core::{driver::schedule::ready::completion, io::fs},
    manifold::file::{self, open},
};

use crate::{abi, context};

enum Stage<'d, const ID: u8> {
    Init(fs::OpenPath),
    Pending(file::Key<'d, open::Operation, ID>),
    Done,
}

#[must_use = "a fiber does nothing unless it is driven"]
pub struct OpenRegular<'app, 'd: 'app, const ID: u8, const N: usize, F>
where
    F: fs::Mode,
{
    access: file::Access<'app, 'd, ID, N, F>,
    stage: Stage<'d, ID>,
}

impl<'app, 'd: 'app, const ID: u8, const N: usize, F> OpenRegular<'app, 'd, ID, N, F>
where
    F: fs::Mode,
{
    pub fn new(access: file::Access<'app, 'd, ID, N, F>, path: fs::OpenPath) -> Self {
        Self {
            access,
            stage: Stage::Init(path),
        }
    }

    fn poll_completion(
        &mut self,
        token: file::Key<'d, open::Operation, ID>,
        wake: completion::Waker<'d>,
    ) -> task::Poll<io::Result<file::Regular>> {
        use dope::manifold::file::{Outcome, open::Done};
        self.stage = Stage::Pending(token);
        match self.access.poll_open(token, wake) {
            Outcome::Done(Done::Opened(file)) => {
                self.stage = Stage::Done;
                task::Poll::Ready(Ok(file))
            }
            Outcome::Done(Done::Failed(error)) => {
                self.stage = Stage::Done;
                task::Poll::Ready(Err(error))
            }
            Outcome::Pending => task::Poll::Pending,
        }
    }
}

impl<const ID: u8, const N: usize, F> Unpin for OpenRegular<'_, '_, ID, N, F> where F: fs::Mode {}

impl<const ID: u8, const N: usize, F> Drop for OpenRegular<'_, '_, ID, N, F>
where
    F: fs::Mode,
{
    fn drop(&mut self) {
        if let Stage::Pending(token) = self.stage {
            self.access.cancel_open(token);
        }
    }
}

impl<'app, 'd: 'app, const ID: u8, const N: usize, F> abi::Fiber<'d>
    for OpenRegular<'app, 'd, ID, N, F>
where
    F: fs::Mode,
{
    type Output = io::Result<file::Regular>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use std::mem::replace;

        use crate::file::already_done;
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let token = match replace(&mut this.stage, Stage::Done) {
            Stage::Done => return task::Poll::Ready(Err(already_done())),
            Stage::Pending(token) => token,
            Stage::Init(path) => {
                let mut driver = cx.as_mut().driver_access();
                match this.access.begin_open(path, &mut driver) {
                    Ok(token) => token,
                    Err(error) => return task::Poll::Ready(Err(error)),
                }
            }
        };
        let wake = cx.as_ref().completion_waker();
        this.poll_completion(token, wake)
    }
}
