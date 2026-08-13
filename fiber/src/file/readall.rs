use std::{io, task};

use dope::{
    core::{
        driver::{retained, schedule::ready::completion},
        io::fs,
    },
    manifold::file::{self, read},
};

use crate::{abi, context};

enum Phase<'d, const ID: u8> {
    Ready(Vec<u8>),
    Init {
        file: file::Regular,
        buffer: Vec<u8>,
    },
    Pending(file::Key<'d, read::Operation, ID>),
    Done,
}

#[must_use = "a fiber does nothing unless it is driven"]
/// Reads the complete contents of a verified regular-file capability.
///
/// An arbitrary descriptor cannot cross this boundary:
///
/// ```compile_fail
/// use std::os::fd::OwnedFd;
/// use dope::manifold::file::Access;
/// use dope_fiber::file::ReadAll;
///
/// fn read_fd<'app, 'd: 'app>(access: Access<'app, 'd, 7, 1>, fd: OwnedFd) {
///     let _ = ReadAll::try_new(access, fd);
/// }
/// ```
pub struct ReadAll<'app, 'd: 'app, const ID: u8, const N: usize, F>
where
    F: fs::Mode,
{
    access: file::Access<'app, 'd, ID, N, F>,
    phase: Phase<'d, ID>,
}

impl<'app, 'd: 'app, const ID: u8, const N: usize, F> ReadAll<'app, 'd, ID, N, F>
where
    F: fs::Mode,
{
    pub fn try_new(
        access: file::Access<'app, 'd, ID, N, F>,
        file: file::Regular,
    ) -> Result<Self, (file::Regular, io::Error)> {
        let Ok(len) = usize::try_from(file.metadata().len()) else {
            return Err((
                file,
                io::Error::new(io::ErrorKind::InvalidInput, "dope::file: file is too large"),
            ));
        };
        let mut buffer = Vec::new();
        if buffer.try_reserve_exact(len).is_err() {
            return Err((file, io::ErrorKind::OutOfMemory.into()));
        }
        let phase = if len == 0 {
            drop(file);
            Phase::Ready(buffer)
        } else {
            Phase::Init { file, buffer }
        };
        Ok(Self { access, phase })
    }

    fn begin(
        &self,
        file: file::Regular,
        buffer: Vec<u8>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<file::Key<'d, read::Operation, ID>, (file::Regular, Vec<u8>, io::Error)> {
        self.access.begin_read(file, buffer, driver)
    }

    fn poll_completion(
        &mut self,
        token: file::Key<'d, read::Operation, ID>,
        wake: completion::Waker<'d>,
    ) -> task::Poll<io::Result<Vec<u8>>> {
        use dope::manifold::file::{Outcome, read::Done};
        self.phase = Phase::Pending(token);
        match self.access.poll_read(token, wake) {
            Outcome::Done((buffer, Done::Complete)) => {
                self.phase = Phase::Done;
                task::Poll::Ready(Ok(buffer))
            }
            Outcome::Done((_buffer, Done::Failed(error))) => {
                self.phase = Phase::Done;
                task::Poll::Ready(Err(error))
            }
            Outcome::Pending => task::Poll::Pending,
        }
    }
}

impl<const ID: u8, const N: usize, F> Unpin for ReadAll<'_, '_, ID, N, F> where F: fs::Mode {}

impl<const ID: u8, const N: usize, F> Drop for ReadAll<'_, '_, ID, N, F>
where
    F: fs::Mode,
{
    fn drop(&mut self) {
        if let Phase::Pending(token) = self.phase {
            self.access.cancel_read(token);
        }
    }
}

impl<'app, 'd: 'app, const ID: u8, const N: usize, F> abi::Fiber<'d> for ReadAll<'app, 'd, ID, N, F>
where
    F: fs::Mode,
{
    type Output = io::Result<Vec<u8>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use std::mem::replace;

        use crate::file::already_done;

        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let token = match replace(&mut this.phase, Phase::Done) {
            Phase::Done => return task::Poll::Ready(Err(already_done())),
            Phase::Ready(buffer) => return task::Poll::Ready(Ok(buffer)),
            Phase::Pending(token) => token,
            Phase::Init { file, buffer } => {
                let begun = {
                    let mut driver = cx.as_mut().driver_access();
                    this.begin(file, buffer, &mut driver)
                };
                match begun {
                    Ok(token) => token,
                    Err((_file, _buffer, error)) => return task::Poll::Ready(Err(error)),
                }
            }
        };
        let wake = cx.as_ref().completion_waker();
        this.poll_completion(token, wake)
    }
}
