use std::{io, marker};

use crate::{
    backend,
    backend::uring::{
        engine::{completions, submit},
        ring, submission,
    },
    driver::flight,
};

pub(super) struct Cancelled;
pub(super) struct Closing;

#[derive(Clone, Copy)]
enum Failure {
    Cancelled,
    Closing,
}

/// Cold-path phase authority. The phase marker has no runtime representation;
/// it only prevents closing work from being submitted before cancellation CQEs
/// have been completely reclaimed.
pub(super) struct Terminal<'a, 'q, 'd, Phase> {
    backend: &'a mut backend::Uring,
    drain: flight::Drain<'q, 'd>,
    phase: marker::PhantomData<Phase>,
}

impl<'a, 'q, 'd, Phase> Terminal<'a, 'q, 'd, Phase> {
    pub(super) fn new(backend: &'a mut backend::Uring, drain: flight::Drain<'q, 'd>) -> Self {
        Self {
            backend,
            drain,
            phase: marker::PhantomData,
        }
    }
}

impl<'a, 'q, 'd> Terminal<'a, 'q, 'd, Cancelled> {
    pub(super) fn cancel(self) -> io::Result<Terminal<'a, 'q, 'd, Closing>> {
        use io::ErrorKind;

        self.backend.ring.submit()?;
        match self.backend.ring.sync_cancel_all() {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        drain_stable(self.backend, &self.drain, Failure::Cancelled)?;
        if !self.backend.tuning.is_quiescent() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope-uring: tuning state survived kernel quiescence",
            ));
        }
        Ok(Terminal::new(self.backend, self.drain))
    }
}

impl Terminal<'_, '_, '_, Closing> {
    pub(super) fn settle(self) -> io::Result<()> {
        while self.backend.lifecycle.has_maintenance() {
            let queued = queue_close_batch(self.backend);
            if queued == 0 {
                self.backend.ring.submit()?;
                continue;
            }
            self.backend.ring.wait(None)?;
            let mut completed = 0;
            while completed < queued {
                completed += drain_visible(self.backend, &self.drain, Failure::Closing)?.closed;
                if completed < queued {
                    self.backend.ring.wait(None)?;
                }
            }
        }
        drain_stable(self.backend, &self.drain, Failure::Closing)?;
        self.backend.ring.buffers().provided().flush();
        Ok(())
    }
}

fn queue_close_batch(backend: &mut backend::Uring) -> usize {
    let mut queued = 0;
    while let Some(work) = backend.lifecycle.pop_close() {
        let slot = work.slot();
        let operation = if work.retires_slot() {
            submission::Submission::retire_at(slot)
        } else {
            submission::Submission::close_at(slot)
        };
        if submit::Writer::new(&mut backend.ring)
            .submit_once(&operation)
            .is_err()
        {
            backend.lifecycle.restore(work);
            break;
        }
        queued += 1;
    }
    queued
}

fn drain_stable(
    backend: &mut backend::Uring,
    drain: &flight::Drain<'_, '_>,
    failure: Failure,
) -> io::Result<()> {
    loop {
        let visible = drain_visible(backend, drain, failure)?;
        if visible.entries != 0 || visible.pending {
            continue;
        }
        if backend.ring.flush_completions()? {
            continue;
        }
        return Ok(());
    }
}

fn drain_visible(
    backend: &mut backend::Uring,
    drain: &flight::Drain<'_, '_>,
    failure: Failure,
) -> io::Result<Drained> {
    let source = ring::Drain::begin(&mut backend.ring);
    let mut entries = 0;
    let mut closed = 0;
    let mut failure_result = None;
    while let Some(completion) = source.next(&mut backend.ring).map(completions::Cqe::new) {
        entries += 1;
        match resolve(backend, drain, completion, failure) {
            Ok(true) => closed += 1,
            Ok(false) => {}
            Err(error) => {
                failure_result = Some(error);
                break;
            }
        }
    }
    let pending = source.finish(&mut backend.ring);
    if let Some(error) = failure_result {
        return Err(error);
    }
    Ok(Drained {
        entries,
        closed,
        pending,
    })
}

struct Drained {
    entries: usize,
    closed: usize,
    pending: bool,
}

fn resolve(
    backend: &mut backend::Uring,
    drain: &flight::Drain<'_, '_>,
    item: completions::Cqe,
    failure: Failure,
) -> io::Result<bool> {
    let driver = drain.driver();
    let disposition = {
        let backend::Uring {
            ring,
            tuning,
            fixed_slots,
            ..
        } = backend;
        let provided = ring.buffers().provided();
        completions::Resolver::new(provided, tuning, fixed_slots, drain).resolve(item)
    };
    match disposition {
        completions::Disposition::Consumed(buffer) => {
            if let Some(buffer) = buffer {
                backend.ring.buffers().provided().defer(buffer);
            }
            Ok(false)
        }
        completions::Disposition::Public(completion) => {
            use crate::backend::uring::capabilities::lifecycle::reclaims;

            reclaims::Reclaim::new(backend, driver).apply(completion);
            Ok(false)
        }
        completions::Disposition::Closed(completion) => {
            let completion = if completion.result() < 0 {
                match failure {
                    Failure::Cancelled => {
                        backend.lifecycle.restore(completion.into_work());
                        None
                    }
                    Failure::Closing => {
                        backend.ring.remove_file(completion.work().slot())?;
                        Some(completion)
                    }
                }
            } else {
                Some(completion)
            };
            let Some(completion) = completion else {
                return Ok(true);
            };
            completion.settle(&mut backend.fixed_slots, driver);
            Ok(true)
        }
    }
}
