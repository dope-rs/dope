use std::{
    io, mem,
    os::fd,
    process, ptr,
    sync::{self, atomic, mpsc},
    thread,
};

use crate::{
    backend::{
        bound,
        kqueue::{self, engine::event, errno, file},
    },
    driver::{self, flight},
};

mod submission;
pub(crate) use submission::Submission;

/// A validated flight key carried opaquely across the file-worker hop.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct TransferKey(flight::raw::Echo);

struct Job {
    submission: file::Submission,
    key: TransferKey,
}

#[derive(Clone, Copy)]
enum Priority {
    File,
    Kevent,
}

#[derive(Clone, Copy)]
struct Wake {
    kq: fd::RawFd,
}

pub(in crate::backend::kqueue) struct Lane {
    jobs: Option<mpsc::SyncSender<Job>>,
    completions: mpsc::Receiver<submission::Completion>,
    ready: sync::Arc<atomic::AtomicUsize>,
    worker: Option<thread::JoinHandle<()>>,
    inflight: usize,
    capacity: usize,
    priority: Priority,
}

const _: () = {
    assert!(mem::size_of::<TransferKey>() == mem::size_of::<flight::raw::Echo>());
    assert!(mem::align_of::<TransferKey>() == mem::align_of::<flight::raw::Echo>());
    assert!(mem::size_of::<Priority>() == 1);
};

// SAFETY: only `Lane::submit` can create this wrapper, after the exact flight
// reservation has been paired with a retained submission. The worker never
// inspects the key and returns it unchanged to the owning reactor.
unsafe impl Send for TransferKey {}

// SAFETY: a job is created only by consuming a retained-owner-bound
// submission. Shutdown joins the worker before that owner can be released.
unsafe impl Send for Job {}

impl Job {
    fn execute(self) -> submission::Completion {
        self.submission.execute(self.key.0)
    }
}

impl Priority {
    fn starts_file(self) -> bool {
        matches!(self, Self::File)
    }

    fn alternate(self) -> Self {
        match self {
            Self::File => Self::Kevent,
            Self::Kevent => Self::File,
        }
    }
}

impl Wake {
    fn trigger(self) {
        let change = libc::kevent {
            ident: kqueue::WAKE_IDENT,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ENABLE,
            fflags: libc::NOTE_TRIGGER,
            data: 0,
            udata: ptr::null_mut(),
        };
        loop {
            let result =
                unsafe { libc::kevent(self.kq, &change, 1, ptr::null_mut(), 0, ptr::null()) };
            if result == 0 {
                return;
            }
            if errno::Errno::last().raw() != libc::EINTR {
                process::abort();
            }
        }
    }
}

impl Lane {
    pub(in crate::backend::kqueue) fn new(capacity: usize, kq: fd::RawFd) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope-kqueue: file queue capacity is zero",
            ));
        }
        let (jobs, worker_jobs) = mpsc::sync_channel::<Job>(capacity);
        let (worker_completions, completions) =
            mpsc::sync_channel::<submission::Completion>(capacity);
        let ready = sync::Arc::new(atomic::AtomicUsize::new(0));
        let worker_ready = sync::Arc::clone(&ready);
        let worker = thread::Builder::new()
            .name("dope-kqueue-file".into())
            .spawn(move || {
                let wake = Wake { kq };
                while let Ok(job) = worker_jobs.recv() {
                    let completion = job.execute();
                    match worker_completions.try_send(completion) {
                        Ok(()) => {}
                        Err(mpsc::TrySendError::Full(_)) => process::abort(),
                        Err(mpsc::TrySendError::Disconnected(_)) => return,
                    }
                    worker_ready.fetch_add(1, atomic::Ordering::Release);
                    wake.trigger();
                }
            })?;
        Ok(Self {
            jobs: Some(jobs),
            completions,
            ready,
            worker: Some(worker),
            inflight: 0,
            capacity,
            priority: Priority::File,
        })
    }

    pub(super) fn submit<'owner, 'd: 'owner>(
        &mut self,
        submission: bound::Bound<'owner, 'd, file::Submission>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        if self.inflight >= self.capacity {
            return Err(driver::SubmitError);
        }
        let Some(jobs) = &self.jobs else {
            return Err(driver::SubmitError);
        };
        let (submission, reservation) = submission.into_parts();
        let job = Job {
            submission,
            key: TransferKey(reservation.key()),
        };
        match jobs.try_send(job) {
            Ok(()) => {
                self.inflight += 1;
                Ok(reservation.commit())
            }
            Err(mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_)) => {
                Err(driver::SubmitError)
            }
        }
    }

    pub(in crate::backend::kqueue) fn starts_batch(&mut self) -> bool {
        let starts_file = self.priority.starts_file();
        self.priority = self.priority.alternate();
        starts_file
    }

    pub(in crate::backend::kqueue) fn ready_count(&self) -> usize {
        self.ready.load(atomic::Ordering::Acquire)
    }

    pub(in crate::backend::kqueue) fn pop(&mut self) -> Option<event::Completion> {
        if self.ready_count() == 0 {
            return None;
        }
        let completion = match self.completions.try_recv() {
            Ok(completion) => completion,
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => process::abort(),
        };
        let previous = self.ready.fetch_sub(1, atomic::Ordering::AcqRel);
        if previous == 0 || self.inflight == 0 {
            process::abort();
        }
        self.inflight -= 1;
        Some(completion.into_event())
    }

    pub(in crate::backend::kqueue) fn shutdown(&mut self) {
        drop(self.jobs.take());
        let Some(worker) = self.worker.take() else {
            return;
        };
        if worker.join().is_err() {
            process::abort();
        }
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        self.shutdown();
        while let Some(completion) = self.pop() {
            drop(completion);
        }
    }
}
