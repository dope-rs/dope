use std::{
    marker, mem, ops,
    os::fd::{self, AsFd as _, AsRawFd as _},
    ptr, time,
};

use dope_sys::{
    Budget, Errno,
    io::{
        reactor,
        reactor::ready::{self, delivery::Event as _},
    },
    table::{
        self, Files as _,
        change::{Changes as _, Filter, Tail as _},
    },
};

use crate::{
    reactor::{CAPACITY, Events, Kernel},
    store::{change::Batch, table::Files},
    system::Queue,
};

pub struct Poll<S, F, const LANES: u8> {
    kq: Queue,
    failure: Option<F>,
    cursor: u8,
    slots: marker::PhantomData<fn() -> S>,
}

impl<S, F, const LANES: u8> fd::AsFd for Poll<S, F, LANES> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.kq.as_fd()
    }
}

impl<S, F, const LANES: u8> ready::Poll<S> for Poll<S, F, LANES>
where
    S: table::raw::Slots,
    F: ready::Failure<Index = S::Index, Missing = S::Missing>,
{
    type Failure = F;
    type Files = Files<S>;
    type Storage = [mem::MaybeUninit<libc::kevent>; CAPACITY];
    type Event<'a> = Kernel<'a>;
    type Events<'a> = Events<'a>;
    fn try_new() -> Result<Self, reactor::Create> {
        const { assert!(LANES > 0) };
        Ok(Self {
            kq: Queue::new()?,
            failure: None,
            cursor: 0,
            slots: marker::PhantomData,
        })
    }

    fn storage() -> Self::Storage {
        [const { mem::MaybeUninit::uninit() }; CAPACITY]
    }

    fn next_lane(&mut self) -> usize {
        let cursor = self.cursor as usize;
        self.cursor = (self.cursor + 1) % LANES;
        cursor
    }

    /// The first retained failure leaves at most RECEIPTS - 1 additional receipts.
    fn revoke(&mut self, files: &mut Self::Files) -> Result<(), ready::Failures<F>> {
        self.check()?;
        while !files.changes().is_empty() {
            let mut tail = files.edit().tail(ready::RECEIPTS);
            if let Err(failures) = self.receipts(tail.changes()) {
                tail.retry();
                return Err(failures);
            }
            tail.commit();
        }
        Ok(())
    }

    fn is_failed(&self) -> bool {
        self.failure.is_some()
    }

    fn check(&self) -> Result<(), F> {
        match self.failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn execute<T, E>(
        &mut self,
        run: impl FnOnce() -> Result<T, E>,
        classify: impl FnOnce(E) -> F,
    ) -> ops::ControlFlow<(), T> {
        if self.is_failed() {
            return ops::ControlFlow::Break(());
        }
        match run() {
            Ok(value) => ops::ControlFlow::Continue(value),
            Err(error) => {
                self.failure = Some(classify(error));
                ops::ControlFlow::Break(())
            }
        }
    }

    fn wait<'a>(
        &mut self,
        files: &mut Self::Files,
        events: &'a mut Self::Storage,
        limit: usize,
        timeout: Option<time::Duration>,
        changes: &mut impl Budget,
    ) -> Result<Self::Events<'a>, F> {
        use dope_sys::table::change::raw::Batch as _;
        use libc::time_t;

        self.check()?;
        let timeout = timeout
            .map(|span| {
                if span.as_secs() > time_t::MAX as u64 {
                    return Err(F::from(ready::Cause::Timeout(span)));
                }
                Ok(libc::timespec {
                    tv_sec: span.as_secs() as time_t,
                    tv_nsec: span.subsec_nanos().into(),
                })
            })
            .transpose()?;
        let mut batch = files.edit().batch(changes);
        let (deletes, updates) = batch.split();
        if !deletes.as_slice().is_empty()
            && let Err(failures) = self.receipts(deletes)
        {
            batch.retry();
            return Err(failures.first);
        }
        let empty = updates.as_slice().is_empty();
        if empty && limit == 0 {
            batch.commit();
            return Ok(Events([].iter()));
        }
        let ready = match Queue::wait(self.kq.as_fd(), &updates, events, limit, timeout.as_ref()) {
            Ok(ready) => {
                batch.commit();
                ready
            }
            Err(errno) => {
                if errno.is_intr() && updates.as_slice().len() <= limit.min(CAPACITY) {
                    batch.commit();
                    &[]
                } else {
                    batch.retry();
                    return Err(self.fail(F::from(ready::Cause::Wait(errno))));
                }
            }
        };
        Ok(Events(ready.iter()))
    }

    /// EV_CLEAR consumes readiness once, preventing repeated shutdown completions.
    fn arm_shutdown(&self, fd: fd::BorrowedFd<'_>, data: S::Data) -> Result<(), Errno> {
        let event = libc::kevent {
            ident: fd.as_raw_fd() as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: ptr::without_provenance_mut(data.into()),
        };
        self.kq.submit(&[event])
    }
}

impl<S, F, const LANES: u8> Poll<S, F, LANES>
where
    S: table::raw::Slots,
    F: ready::Failure<Index = S::Index, Missing = S::Missing>,
{
    /// Each bounded change owns one receipt slot, so no application readiness
    /// can be consumed. Only an absent deletion is an already completed change.
    fn receipts(&mut self, changes: Batch<'_, S>) -> Result<(), ready::Failures<F>> {
        use dope_sys::table::change::raw::Batch as _;
        use ready::{Failures, RECEIPTS};

        let mut receipts = [mem::MaybeUninit::uninit(); RECEIPTS];
        for batch in changes.receipts() {
            let staged = batch.as_slice();
            loop {
                let received =
                    match Queue::wait(self.kq.as_fd(), &batch, &mut receipts, staged.len(), None) {
                        Ok(received) => received,
                        Err(errno) if errno.is_intr() => continue,
                        Err(errno) => {
                            return Err(self.fail(F::from(ready::Cause::Changes(errno))).into());
                        }
                    };
                let mut failures = received.iter().zip(staged).filter_map(|(receipt, change)| {
                    let errno = match Kernel(receipt).error() {
                        None => return None,
                        Some(errno)
                            if change.flags & libc::EV_DELETE != 0
                                && errno.code().get() == libc::ENOENT =>
                        {
                            return None;
                        }
                        Some(errno) => errno,
                    };
                    let filter = if change.filter == libc::EVFILT_READ {
                        Filter::Read
                    } else {
                        Filter::Write
                    };
                    Some(F::revoke(receipt.ident, filter, errno))
                });
                let Some(first) = failures.next() else {
                    break;
                };
                let mut rest = [const { None }; RECEIPTS - 1];
                for (slot, failure) in rest.iter_mut().zip(failures) {
                    *slot = Some(failure);
                }
                return Err(Failures {
                    first: self.fail(first),
                    rest,
                });
            }
        }
        Ok(())
    }
}

impl<S, F: Copy, const LANES: u8> Poll<S, F, LANES> {
    fn fail(&mut self, error: F) -> F {
        self.failure = Some(error);
        error
    }
}
