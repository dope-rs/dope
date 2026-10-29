use std::convert;

use dope_core::{
    driver,
    driver::{
        result, retained,
        retained::flight,
        route,
        route::{Identity, indices, table},
        schedule::clock,
    },
    io::{
        event::net::{connect, send, socket::Completion},
        fd::{handles, outbound},
        socket,
        socket::Open,
    },
};
use o3::{
    depot,
    depot::{
        arena::slab::{self, entries, external::errors, lookup},
        heap::keyed,
        index,
        queue::indexed::{exact, fifo},
    },
};

use crate::link::{
    egress::data,
    oneshot::{
        self, Busy, Cancellation, Dispatch, Error, Pool, Progress, Request, Unsent, phase, routes,
    },
};

pub(super) struct Access<'a, 'd, ID: Identity, A: 'd, P: socket::Prepared, B: 'd> {
    index: routes::Index,
    slot: route::raw::Slot<'a, oneshot::raw::Cell<'d, A, P, B>, ID>,
    routes: &'a routes::Routes<'d, ID>,
    retry: &'a mut fifo::List<routes::Local<'d, ID>, indices::SlotIndex, exact::Policy>,
    flights: &'a flight::Slots<'d, ID>,
    deadlines: &'a mut keyed::Min<routes::Index, routes::Due<'d>>,
}

impl<'a, 'd, ID: Identity, A: 'd, P: socket::Prepared, B: 'd> Access<'a, 'd, ID, A, P, B>
where
    B: data::raw::Payload<'d> + data::raw::StableOwner<'d>,
{
    pub(super) fn new(live: Allocated<'a, 'd, ID, A, P, B>) -> Self {
        let (pool, index) = live.into_inner();
        // SAFETY: the consumed Pool admission retains its actual slots allocation.
        let slot = unsafe { depot::raw::Proof::new((pool.slots.raw(), index.local())) }.into();
        Self {
            index,
            slot,
            routes: &pool.routes,
            retry: &mut pool.retries,
            flights: &pool.flights,
            deadlines: &mut pool.deadlines,
        }
    }

    pub(super) fn send(
        self,
        fds: &outbound::Reserved<'d, ID>,
        req: Request<'d, A, P, B>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Dispatch<Request<'d, A, P, B>> {
        let vacant = match self.slot.vacancy() {
            Ok(vacant) => vacant,
            Err(slab::Vacancy::Full) => {
                return Dispatch::Unsent(Unsent::Busy {
                    request: req,
                    cause: Busy::Slot,
                });
            }
            Err(slab::Vacancy::IdentityExhausted) => {
                return Dispatch::Unsent(Unsent::Exhausted(req, errors::Exhausted));
            }
        };
        let local = self.routes.bind(self.index, vacant.key().generation());
        let target = local.physical();
        let Some(fd) = fds.bind(target) else {
            return Dispatch::Unsent(Unsent::Busy {
                request: req,
                cause: Busy::Descriptor,
            });
        };
        let socket = req.address.stream_spec();
        let open = match Open::try_new(driver, self.flights, fd, socket) {
            Ok(open) => open.commit(),
            Err(error) => {
                return Dispatch::Unsent(match error.cause() {
                    result::SubmitCause::Full => Unsent::Busy {
                        request: req,
                        cause: Busy::Submit(error),
                    },
                    result::SubmitCause::IdentityExhausted => Unsent::Rejected {
                        request: req,
                        cause: error,
                    },
                });
            }
        };
        let (cell, until) = oneshot::raw::Cell::new(req, open);
        // SAFETY: this Pool constructs both extents from cap and retains them;
        // the actual vacancy admits index in their shared physical domain.
        let deadline =
            unsafe { depot::raw::Proof::new(((self.deadlines, vacant), self.index)) }.into();
        deadline
            .replace(routes::Due::Connect {
                epoch: local.parts().epoch(),
                at: until,
            })
            .stage(cell)
            .publish();
        Dispatch::Sent
    }

    /// Releases an accessible owner; empty or leaked-borrow positions retain none
    /// that this exclusive scan can reach.
    pub(super) fn finish(self) -> Option<A> {
        match Entry::try_at(self) {
            Ok(entry) => entry.take(),
            Err(
                lookup::Error::Free { .. }
                | lookup::Error::Reserved { .. }
                | lookup::Error::Index { .. }
                | lookup::Error::Retired { .. }
                | lookup::Error::Busy { .. },
            ) => None,
        }
    }
}

pub(super) struct Entry<'a, 'd, ID: Identity, A: 'd, P: socket::Prepared, B: 'd> {
    current: table::Current<'a, oneshot::raw::Cell<'d, A, P, B>, ID>,
    local: routes::Local<'d, ID>,
    retry: exact::Entry<'a, routes::Local<'d, ID>, indices::SlotIndex>,
    flights: &'a flight::Slots<'d, ID>,
    deadlines: &'a mut keyed::Min<routes::Index, routes::Due<'d>>,
}

impl<'a, 'd, ID: Identity, A: 'd, P, B: 'd> Entry<'a, 'd, ID, A, P, B>
where
    P: socket::Prepared,
    B: data::raw::Payload<'d> + data::raw::StableOwner<'d>,
{
    pub(super) fn try_new(
        access: Access<'a, 'd, ID, A, P, B>,
        epoch: route::Epoch,
    ) -> Result<Self, errors::Lookup<route::Epoch, ID, ()>> {
        use o3::depot::raw::Proof;
        let current = access.slot.try_key(epoch)?;
        let local = access.routes.bind(access.index, epoch);
        // SAFETY: Pool constructs and retains both fixed extents from one capacity;
        // Current proves this local position before the paired FIFO projection.
        let retry = unsafe { Proof::new((access.retry, access.index.local())) }.into();
        Ok(Self {
            current,
            local,
            retry,
            flights: access.flights,
            deadlines: access.deadlines,
        })
    }

    pub(super) fn socket(
        mut self,
        done: Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Progress<A> {
        let target = self.local.physical();
        let step = self
            .current
            .get_mut()
            .socket(done, target, self.flights, driver);
        match step {
            phase::Socket::Pending => Progress::Pending,
            phase::Socket::Cancelled(close) => self.finish(Err(Error::Abandoned), close, driver),
            phase::Socket::Failed(error, close) => self.finish(Err(error), close, driver),
            phase::Socket::Finished(error) => self.finish_done(Err(error)),
            phase::Socket::Stale => Progress::Idle,
            phase::Socket::Retry => {
                self.defer();
                Progress::Pending
            }
        }
    }

    pub(super) fn try_at(
        access: Access<'a, 'd, ID, A, P, B>,
    ) -> Result<Self, lookup::Error<(), convert::Infallible>> {
        use o3::depot::raw::Proof;
        let current = access.slot.try_entry()?;
        let local = access.routes.bind(access.index, current.key().epoch());
        // SAFETY: the original Pool retains matching immutable allocation extents,
        // and Current preserves this selected physical slot through projection.
        let retry = unsafe { Proof::new((access.retry, access.index.local())) }.into();
        Ok(Self {
            current,
            local,
            retry,
            flights: access.flights,
            deadlines: access.deadlines,
        })
    }

    pub(super) fn connect(
        mut self,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<Progress<A>, connect::Completion<'d>> {
        let now = driver.driver_ref().clock().now();
        let target = self.local.physical();
        let step = self
            .current
            .get_mut()
            .connect(done, now, target, self.flights, driver)?;
        let (until, sent) = match step {
            phase::Progress::Connected(until, sent) => (until, sent),
            phase::Progress::Failed(error, close) => {
                return Ok(self.finish(Err(error), close, driver));
            }
            phase::Progress::Cancelled(close) => {
                return Ok(self.finish(Err(Error::Abandoned), close, driver));
            }
        };
        let epoch = self.local.parts().epoch();
        // SAFETY: Entry retains the actual Current and its paired Pool heap;
        // successful admission tied local to this original physical slot.
        let deadline = unsafe {
            depot::raw::Proof::new(((&mut *self.deadlines, self.current), self.local.index()))
        }
        .into();
        self.current = deadline.replace(routes::Due::Send { epoch, at: until });
        Ok(self.settle(sent, driver))
    }

    pub(super) fn send(
        mut self,
        done: send::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<Progress<A>, send::Completion<'d>> {
        let target = self.local.physical();
        let step = self
            .current
            .get_mut()
            .send(done, target, self.flights, driver)?;
        Ok(self.settle(step, driver))
    }

    pub(super) fn cancel(
        mut self,
        reason: Cancellation,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress<A> {
        // SAFETY: Current and the heap remain borrowed from the same admitted Pool.
        let deadline = unsafe {
            depot::raw::Proof::new(((&mut *self.deadlines, self.current), self.local.index()))
        }
        .into();
        self.current = deadline.clear();
        let cancel = self.current.get_mut().cancel(driver);
        let Some((owner, cancel)) = cancel else {
            return Progress::Idle;
        };
        let cancel = match cancel {
            Ok(cancel) => cancel,
            Err(failure) => {
                self.retry.unlink(self.local);
                driver.rejected(failure);
                return Progress::Complete {
                    authority: owner,
                    result: Err(Error::Cancellation((reason, failure))),
                };
            }
        };
        match cancel {
            phase::Cancellation::Idle(close) => {
                self.retry.unlink(self.local);
                self.current.discard();
                close.close(driver);
            }
            phase::Cancellation::Pending => {}
            phase::Cancellation::Retry(_) => self.defer(),
        }
        Progress::Complete {
            authority: owner,
            result: Err(reason.into()),
        }
    }

    fn settle(
        self,
        progress: phase::SendProgress<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Progress<A> {
        match progress {
            phase::SendProgress::Complete(close) => self.finish(Ok(()), close, driver),
            phase::SendProgress::Submitted => Progress::Pending,
            phase::SendProgress::Retry(_) => {
                self.defer();
                Progress::Pending
            }
            phase::SendProgress::Cancelled(close) => {
                self.finish(Err(Error::Abandoned), close, driver)
            }
            phase::SendProgress::Failed(error, close) => self.finish(Err(error), close, driver),
        }
    }

    fn defer(mut self) {
        self.retry.schedule(self.local);
    }

    fn finish(
        self,
        result: Result<(), Error>,
        close: handles::Descriptor<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress<A> {
        let done = self.finish_done(result);
        close.close(driver);
        done
    }

    fn take(mut self) -> Option<A> {
        // SAFETY: Current and the heap remain borrowed from the same admitted Pool.
        let deadline = unsafe {
            depot::raw::Proof::new(((&mut *self.deadlines, self.current), self.local.index()))
        }
        .into();
        self.current = deadline.clear();
        self.retry.unlink(self.local);
        self.current.take().authority
    }

    fn finish_done(self, result: Result<(), Error>) -> Progress<A> {
        match self.take() {
            Some(owner) => Progress::Complete {
                authority: owner,
                result,
            },
            None => Progress::Idle,
        }
    }

    pub(super) fn expire(
        self,
        now: clock::Moment<'d>,
        due: routes::Due<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress<A> {
        self.cancel(due.timeout(now), driver)
    }

    pub(super) fn retry(
        mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<Progress<A>, result::SubmitError> {
        let target = self.local.physical();
        let retry = self.current.get_mut().retry(target, self.flights, driver);
        let error = match retry {
            phase::Retry::Idle => return Ok(Progress::Idle),
            phase::Retry::Cancel(Err(cause)) => {
                self.retry.unlink(self.local);
                driver.rejected(cause);
                return Ok(Progress::Pending);
            }
            phase::Retry::Cancel(Ok(step)) => match step {
                phase::Cancellation::Idle(close) => {
                    return Ok(self.finish(Err(Error::Abandoned), close, driver.driver()));
                }
                phase::Cancellation::Pending => return Ok(Progress::Pending),
                phase::Cancellation::Retry(error) => error,
            },
            phase::Retry::Send(phase::SendProgress::Retry(error)) => error,
            phase::Retry::Send(progress) => return Ok(self.settle(progress, driver)),
            phase::Retry::Connect(step) => match step {
                phase::Dial::Pending => return Ok(Progress::Pending),
                phase::Dial::Failed(error, close) => {
                    return Ok(self.finish(Err(error), close, driver));
                }
                phase::Dial::Retry(error) => error,
            },
        };
        self.defer();
        Err(error)
    }
}

type Allocated<'a, 'd, ID, A, P, B> = depot::raw::Proof<
    (&'a mut Pool<'d, ID, A, P, B>, routes::Index),
    index::Initialized<entries::Allocated>,
>;
