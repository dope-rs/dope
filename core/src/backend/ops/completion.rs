use std::io;
use std::time::Duration;

use crate::backend::Backend;
use crate::driver::DriverRef;
use crate::io::Event;

pub(crate) trait CompletionBackend {
    fn drain<'d>(
        backend: &mut Backend,
        reference: DriverRef<'d>,
        buf: &mut [Option<Event<'d>>],
    ) -> usize;
    fn wait(backend: &mut Backend, timeout: Option<Duration>) -> io::Result<()>;
}

#[cfg(target_os = "linux")]
mod linux {
    use io_uring::types::{SubmitArgs, Timespec};
    use libc::ETIME;

    use crate::backend::uring::driver::Disposition;
    use crate::io::Cqe;

    use super::{Backend, CompletionBackend, DriverRef, Duration, Event, io};

    impl CompletionBackend for Backend {
        fn drain<'d>(
            backend: &mut Backend,
            reference: DriverRef<'d>,
            buf: &mut [Option<Event<'d>>],
        ) -> usize {
            let Backend {
                uring,
                setsockopt,
                files,
                provided,
                routes,
                ..
            } = backend;
            let mut n = 0;
            {
                let mut cq = uring.completion();
                while n < buf.len() {
                    let Some(item) = cq.next() else { break };
                    let result = item.result();
                    let flags = item.flags();
                    let user_data = match Backend::complete_cqe(
                        setsockopt,
                        files,
                        routes,
                        item.user_data(),
                        result,
                        flags,
                    ) {
                        Disposition::Drop | Disposition::Internal => continue,
                        Disposition::DropBuffer(bid) => {
                            provided.defer_completion(bid);
                            continue;
                        }
                        Disposition::Public(user_data) => user_data,
                    };
                    let event = Event::from_cqe(
                        Cqe::new(user_data, result, flags),
                        reference,
                        |len, bid| Some(provided.complete(bid, len as usize)),
                    );
                    if let Ok(event) = event {
                        buf[n] = Some(event);
                        n += 1;
                    }
                }
                cq.sync();
            }
            backend.flush_deferred_close();
            backend.flush_ready_create();
            backend.provided.flush();
            n
        }

        fn wait(backend: &mut Backend, timeout: Option<Duration>) -> io::Result<()> {
            backend.flush_deferred_close();
            backend.flush_ready_create();
            backend.provided.flush();
            match timeout {
                Some(timeout) => {
                    let timespec = Timespec::from(timeout);
                    let args = SubmitArgs::new().timespec(&timespec);
                    match backend.uring.submitter().submit_with_args(1, &args) {
                        Ok(_) => Ok(()),
                        Err(error) if error.raw_os_error() == Some(ETIME) => Ok(()),
                        Err(error) => Err(error),
                    }
                }
                None => backend.uring.submitter().submit_and_wait(1).map(|_| ()),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use std::mem::MaybeUninit;
    use std::slice::from_raw_parts;

    use libc::kevent;

    use crate::backend::kqueue::driver::MAX_DRAIN_PER_FD;
    use crate::backend::kqueue::driver::pending::PendingCompletion;
    use crate::backend::kqueue::driver::read::dispatch::Dispatch;
    use crate::driver::token::SHUTDOWN;
    use crate::io::provided::raw::buffer::BufferId;
    use crate::io::{BUFFER, BUFFER_SHIFT, Cqe, MORE};

    use super::{Backend, CompletionBackend, DriverRef, Duration, Event, io};

    struct PendingCqe {
        cqe: Cqe,
        id: Option<BufferId>,
    }

    impl CompletionBackend for Backend {
        fn drain<'d>(
            backend: &mut Backend,
            reference: DriverRef<'d>,
            buf: &mut [Option<Event<'d>>],
        ) -> usize {
            if backend.pending.is_empty() {
                let _ = <Backend as CompletionBackend>::wait(backend, Some(Duration::ZERO));
            }
            let mut n = 0;
            while n < buf.len() {
                let Some(pending) = backend.pending.pop_front() else {
                    break;
                };
                let PendingCqe { cqe, id } = match pending {
                    PendingCompletion::Accept { ud, result, more } => PendingCqe {
                        cqe: Cqe::new(ud.raw(), result, if more { MORE } else { 0 }),
                        id: None,
                    },
                    PendingCompletion::Recv {
                        ud,
                        result,
                        more,
                        bid,
                    } => {
                        let mut flags = if more { MORE } else { 0 };
                        if let Some(id) = bid.as_ref() {
                            flags |= BUFFER | (((*id).into_raw() as u32) << BUFFER_SHIFT);
                        }
                        PendingCqe {
                            cqe: Cqe::new(ud.raw(), result, flags),
                            id: bid,
                        }
                    }
                    PendingCompletion::Write { ud, result } => PendingCqe {
                        cqe: Cqe::new(ud.raw(), result, 0),
                        id: None,
                    },
                    PendingCompletion::Create { ud, result, .. } => PendingCqe {
                        cqe: Cqe::new(ud.raw(), result, 0),
                        id: None,
                    },
                    PendingCompletion::Timer { ud } => PendingCqe {
                        cqe: Cqe::new(ud.raw(), 0, 0),
                        id: None,
                    },
                    PendingCompletion::Shutdown => PendingCqe {
                        cqe: Cqe::new(SHUTDOWN.raw(), 0, 0),
                        id: None,
                    },
                };
                let event = Event::from_cqe(cqe, reference, |len, bid| {
                    id.map(|id| {
                        debug_assert_eq!(id.into_raw(), bid);
                        backend.backing.complete(id, len as usize)
                    })
                });
                if let Ok(event) = event {
                    buf[n] = Some(event);
                    n += 1;
                }
            }
            n
        }

        fn wait(backend: &mut Backend, timeout: Option<Duration>) -> io::Result<()> {
            backend.resume_pending();
            let mut events: [MaybeUninit<kevent>; 64] = [const { MaybeUninit::uninit() }; 64];
            if backend.pending.remaining_capacity() < events.len() * MAX_DRAIN_PER_FD {
                return Ok(());
            }
            let n = backend.kevent_call(&mut events, timeout)?;
            let ready = unsafe { from_raw_parts(events.as_ptr().cast::<kevent>(), n) };
            for event in ready {
                backend.dispatch_event(event);
            }
            Ok(())
        }
    }
}
