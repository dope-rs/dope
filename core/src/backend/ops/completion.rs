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

    use super::{Backend, CompletionBackend, DriverRef, Duration, Event, io};
    use crate::backend::uring::driver::Disposition;
    use crate::io::Cqe;

    impl CompletionBackend for Backend {
        fn drain<'d>(
            backend: &mut Backend,
            reference: DriverRef<'d>,
            buf: &mut [Option<Event<'d>>],
        ) -> usize {
            let Backend {
                ring,
                setsockopt,
                files,
                routes,
                ..
            } = backend;
            let (uring, provided) = ring.split();
            let mut n = 0;
            {
                let mut cq = uring.completion();
                while n < buf.len() {
                    let Some(item) = cq.next() else { break };
                    let result = item.result();
                    let flags = item.flags();
                    let token = match Backend::complete_cqe(
                        provided,
                        setsockopt,
                        files,
                        routes,
                        item.user_data(),
                        result,
                        flags,
                    ) {
                        Disposition::Drop | Disposition::Internal => continue,
                        Disposition::DropBuffer(buffer) => {
                            provided.defer(buffer);
                            continue;
                        }
                        Disposition::Public(token) => token,
                    };
                    let event =
                        Event::from_cqe(Cqe::new(token, result, flags), reference, |len, bid| {
                            Some(provided.complete(bid, len as usize))
                        });
                    if let Ok(event) = event {
                        buf[n] = Some(event);
                        n += 1;
                    }
                }
                cq.sync();
            }
            backend.flush_deferred_close();
            backend.flush_ready_create();
            backend.ring.provided_mut().flush();
            n
        }

        fn wait(backend: &mut Backend, timeout: Option<Duration>) -> io::Result<()> {
            backend.flush_deferred_close();
            backend.flush_ready_create();
            backend.ring.provided_mut().flush();
            match timeout {
                Some(timeout) => {
                    let timespec = Timespec::from(timeout);
                    let args = SubmitArgs::new().timespec(&timespec);
                    match backend.ring.io().submitter().submit_with_args(1, &args) {
                        Ok(_) => Ok(()),
                        Err(error) if error.raw_os_error() == Some(ETIME) => Ok(()),
                        Err(error) => Err(error),
                    }
                }
                None => backend.ring.io().submitter().submit_and_wait(1).map(|_| ()),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use std::mem::MaybeUninit;
    use std::slice::from_raw_parts;

    use libc::kevent;

    use super::{Backend, CompletionBackend, DriverRef, Duration, Event, io};
    use crate::backend::RecvBuffer;
    use crate::backend::kqueue::driver::MAX_DRAIN_PER_FD;
    use crate::backend::kqueue::driver::pending::PendingCompletion;
    use crate::backend::kqueue::driver::read::dispatch::Dispatch;
    use crate::driver::token::SHUTDOWN;
    use crate::io::{BUFFER, BUFFER_SHIFT, Cqe, MORE};

    struct PendingCqe {
        cqe: Cqe,
        buffer: Option<RecvBuffer>,
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
                let PendingCqe { cqe, buffer } = match pending {
                    PendingCompletion::Accept { ud, result, more } => PendingCqe {
                        cqe: Cqe::new(ud, result, if more { MORE } else { 0 }),
                        buffer: None,
                    },
                    PendingCompletion::Recv {
                        ud,
                        result,
                        more,
                        buffer,
                    } => {
                        let mut flags = if more { MORE } else { 0 };
                        if let Some(buffer) = buffer.as_ref() {
                            flags |= BUFFER | ((buffer.raw() as u32) << BUFFER_SHIFT);
                        }
                        PendingCqe {
                            cqe: Cqe::new(ud, result, flags),
                            buffer,
                        }
                    }
                    PendingCompletion::Write { ud, result } => PendingCqe {
                        cqe: Cqe::new(ud, result, 0),
                        buffer: None,
                    },
                    PendingCompletion::Create { ud, result, .. } => PendingCqe {
                        cqe: Cqe::new(ud, result, 0),
                        buffer: None,
                    },
                    PendingCompletion::Timer { ud } => PendingCqe {
                        cqe: Cqe::new(ud, 0, 0),
                        buffer: None,
                    },
                    PendingCompletion::Shutdown => PendingCqe {
                        cqe: Cqe::new(SHUTDOWN, 0, 0),
                        buffer: None,
                    },
                };
                let event = Event::from_cqe(cqe, reference, |len, bid| {
                    buffer.map(|buffer| {
                        debug_assert_eq!(buffer.raw(), bid);
                        backend.recv.complete(buffer, len as usize)
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
