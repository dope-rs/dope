use std::io;
use std::time::Duration;

use super::DriverContext;
use crate::io::Cqe;

pub trait Completion {
    fn drain(&mut self, buf: &mut [Cqe]) -> usize;
    fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()>;
}

cfg_select! {
    target_os = "linux" => {
        use io_uring::types::{SubmitArgs, Timespec};

        use crate::backend::uring::driver::{Disposition, Uring};

        impl Completion for DriverContext<'_, '_> {
            fn drain(&mut self, buf: &mut [Cqe]) -> usize {
                self.flush_returned_buffers();
                let state = self.backend();
                let mut n = 0;
                {
                    let Uring {
                        uring,
                        setsockopt,
                        files,
                        provided,
                        routes,
                        ..
                    } = state;
                    let mut cq = uring.completion();
                    while n < buf.len() {
                        let Some(item) = cq.next() else { break };
                        let result = item.result();
                        let user_data = match Uring::complete_cqe(
                            setsockopt,
                            files,
                            routes,
                            item.user_data(),
                            result,
                            item.flags(),
                        ) {
                            Disposition::Drop | Disposition::Internal => continue,
                            Disposition::DropBuffer(bid) => {
                                provided.defer(bid);
                                continue;
                            }
                            Disposition::Public(user_data) => user_data,
                        };
                        buf[n] = Cqe {
                            user_data,
                            result,
                            flags: item.flags(),
                        };
                        n += 1;
                    }
                    cq.sync();
                }
                state.flush_deferred_close();
                state.flush_ready_create();
                state.provided.flush();
                n
            }

            fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
                self.flush_returned_buffers();
                let state = self.backend();
                state.flush_deferred_close();
                state.flush_ready_create();
                state.provided.flush();
                match timeout {
                    Some(timeout) => {
                        let timespec = Timespec::from(timeout);
                        let args = SubmitArgs::new().timespec(&timespec);
                        match state.uring.submitter().submit_with_args(1, &args) {
                            Ok(_) => Ok(()),
                            Err(error) if error.raw_os_error() == Some(libc::ETIME) => Ok(()),
                            Err(error) => Err(error),
                        }
                    }
                    None => state.uring.submitter().submit_and_wait(1).map(|_| ()),
                }
            }
        }
    }
    _ => {
        use std::mem::MaybeUninit;
        use std::slice;

        use crate::backend::kqueue::driver::MAX_DRAIN_PER_FD;
        use crate::backend::kqueue::driver::pending::PendingCompletion;
        use crate::backend::kqueue::driver::read::dispatch::Dispatch;
        use crate::driver::token::SHUTDOWN;
        use crate::io::{BUFFER, BUFFER_SHIFT, MORE};

        impl Completion for DriverContext<'_, '_> {
            fn drain(&mut self, buf: &mut [Cqe]) -> usize {
                self.flush_returned_buffers();
                if self.backend_ref().pending.is_empty() {
                    let _ = Completion::wait(self, Some(Duration::ZERO));
                }
                let state = self.backend();
                let mut n = 0;
                while n < buf.len() {
                    let Some(pending) = state.pending.pop_front() else {
                        break;
                    };
                    buf[n] = match pending {
                        PendingCompletion::Accept { ud, result, more } => Cqe {
                            user_data: ud.raw(),
                            result,
                            flags: if more { MORE } else { 0 },
                        },
                        PendingCompletion::Recv {
                            ud,
                            result,
                            more,
                            bid,
                        } => {
                            let mut flags = if more { MORE } else { 0 };
                            if let Some(bid) = bid {
                                flags |= BUFFER | ((bid as u32) << BUFFER_SHIFT);
                            }
                            Cqe {
                                user_data: ud.raw(),
                                result,
                                flags,
                            }
                        }
                        PendingCompletion::Write { ud, result } => Cqe {
                            user_data: ud.raw(),
                            result,
                            flags: 0,
                        },
                        PendingCompletion::Create { ud, result, .. } => Cqe {
                            user_data: ud.raw(),
                            result,
                            flags: 0,
                        },
                        PendingCompletion::Timer { ud } => Cqe {
                            user_data: ud.raw(),
                            result: 0,
                            flags: 0,
                        },
                        PendingCompletion::Shutdown => Cqe {
                            user_data: SHUTDOWN.raw(),
                            result: 0,
                            flags: 0,
                        },
                    };
                    n += 1;
                }
                n
            }

            fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
                self.flush_returned_buffers();
                let state = self.backend();
                state.resume_pending();
                let mut events: [MaybeUninit<libc::kevent>; 64] = [const { MaybeUninit::uninit() }; 64];
                if state.pending.remaining_capacity() < events.len() * MAX_DRAIN_PER_FD {
                    return Ok(());
                }
                let n = state.kevent_call(&mut events, timeout)?;
                let ready = unsafe { slice::from_raw_parts(events.as_ptr().cast::<libc::kevent>(), n) };
                for event in ready {
                    state.dispatch_event(event);
                }
                Ok(())
            }
        }
    }
}
