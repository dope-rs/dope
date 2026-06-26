use std::{io, time::Duration};

use io_uring::types::{SubmitArgs, Timespec};

use super::Driver;
use super::sqe::Sqe;
use crate::{Cqe, Drive};

impl Drive for Driver {
    type Sqe = Sqe;

    fn push(&mut self, sqe: Self::Sqe) -> Result<(), crate::backend::PushError> {
        let entry = sqe.entry();
        if self.try_push(entry).is_ok() {
            return Ok(());
        }
        self.uring.submit().map_err(|_| crate::backend::PushError)?;
        self.try_push(entry)
    }

    fn submit_to_drain(&mut self) -> bool {
        self.uring.submit().is_ok()
    }

    fn drain(&mut self, buf: &mut [Cqe]) -> usize {
        let mut n = 0;
        {
            let Self { uring, setsockopt, .. } = self;
            let mut cq = uring.completion();
            while n < buf.len() {
                let Some(item) = cq.next() else { break };
                let user_data = item.user_data();
                if Self::release_setsockopt(setsockopt, user_data) {
                    continue;
                }
                buf[n] = Cqe {
                    user_data,
                    result: item.result(),
                    flags: item.flags(),
                };
                n += 1;
            }
            cq.sync();
        }
        self.provided.flush();
        n
    }

    fn park(&mut self, timeout: Duration) -> io::Result<()> {
        self.flush_deferred_close();
        let ts = Timespec::from(timeout);
        let args = SubmitArgs::new().timespec(&ts);
        match self.uring.submitter().submit_with_args(1, &args) {
            Ok(_) => Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) => Err(e),
        }
    }
}