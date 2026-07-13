use std::{io, time::Duration};

use io_uring::types::{SubmitArgs, Timespec};

use super::Driver;
use super::sqe::Sqe;
use crate::{Cqe, Drive};

impl Drive for Driver {
    type Sqe = Sqe;

    fn push(&self, sqe: Self::Sqe) -> Result<(), crate::backend::PushError> {
        // SAFETY: leaf.
        unsafe { self.inner() }.push_sqe(sqe)
    }

    fn submit_to_drain(&self) -> bool {
        // SAFETY: leaf.
        unsafe { self.inner() }.uring.submit().is_ok()
    }

    fn drain(&self, buf: &mut [Cqe]) -> usize {
        // SAFETY: leaf.
        let this = unsafe { self.inner() };
        let mut n = 0;
        {
            let super::Inner { uring, setsockopt, .. } = this;
            let mut cq = uring.completion();
            while n < buf.len() {
                let Some(item) = cq.next() else { break };
                let user_data = item.user_data();
                if super::Inner::release_setsockopt(setsockopt, user_data) {
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
        this.provided.flush();
        n
    }

    fn park(&self, timeout: Duration) -> io::Result<()> {
        // SAFETY: leaf.
        let this = unsafe { self.inner() };
        this.flush_deferred_close();
        let ts = Timespec::from(timeout);
        let args = SubmitArgs::new().timespec(&ts);
        match this.uring.submitter().submit_with_args(1, &args) {
            Ok(_) => Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) => Err(e),
        }
    }
}