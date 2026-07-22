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
    use std::ptr::NonNull;

    use io_uring::types::{SubmitArgs, Timespec};

    use crate::backend::uring::driver::Disposition;
    use crate::io::Cqe;
    use crate::io::provided::ProvidedLease;

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
                    let user_data = match Backend::complete_cqe(
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
                    let event =
                        Event::from_cqe(Cqe::new(user_data, result, item.flags()), |len, bid| {
                            let (ptr, len) = provided.ptr_len(bid, len as usize);
                            let ptr = unsafe { NonNull::new_unchecked(ptr.cast_mut()) };
                            unsafe { ProvidedLease::from_raw_completion(reference, bid, ptr, len) }
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
                        Err(error) if error.raw_os_error() == Some(libc::ETIME) => Ok(()),
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
    use std::slice;

    use crate::backend::kqueue::driver::pending::PendingCompletion;
    use crate::backend::kqueue::driver::read::dispatch::Dispatch;
    use crate::driver::token::SHUTDOWN;
    use crate::io::provided::ProvidedLease;
    use crate::io::{BUFFER, BUFFER_SHIFT, Cqe, MORE};

    use super::{Backend, CompletionBackend, DriverRef, Duration, Event, io};

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
                let cqe = match pending {
                    PendingCompletion::Accept { ud, result, more } => {
                        Cqe::new(ud.raw(), result, if more { MORE } else { 0 })
                    }
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
                        Cqe::new(ud.raw(), result, flags)
                    }
                    PendingCompletion::Write { ud, result } => Cqe::new(ud.raw(), result, 0),
                    PendingCompletion::Create { ud, result, .. } => Cqe::new(ud.raw(), result, 0),
                    PendingCompletion::Timer { ud } => Cqe::new(ud.raw(), 0, 0),
                    PendingCompletion::Shutdown => Cqe::new(SHUTDOWN.raw(), 0, 0),
                };
                let event = Event::from_cqe(cqe, |len, bid| {
                    let (ptr, len) = unsafe { backend.backing.ptr_len(bid, len as usize) };
                    unsafe { ProvidedLease::from_raw_completion(reference, bid, ptr, len) }
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
            let mut events: [MaybeUninit<libc::kevent>; 64] = [const { MaybeUninit::uninit() }; 64];
            if backend.pending.remaining_capacity()
                < events.len() * crate::backend::kqueue::driver::MAX_DRAIN_PER_FD
            {
                return Ok(());
            }
            let n = backend.kevent_call(&mut events, timeout)?;
            let ready = unsafe { slice::from_raw_parts(events.as_ptr().cast::<libc::kevent>(), n) };
            for event in ready {
                backend.dispatch_event(event);
            }
            Ok(())
        }
    }
}
