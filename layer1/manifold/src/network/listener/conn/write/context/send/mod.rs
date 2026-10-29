use std::{convert::Infallible, ops::ControlFlow, task};

use conn::write;
use dope_core::driver::{
    retained::{self, flight},
    route::Identity,
    schedule::app,
};
use dope_net::{
    link::{egress, pool::pending, slot::types},
    wire,
};
use egress::{
    data,
    reserve::{self, error::Full, limit::Limit},
};
use write::{context::modes, error};
use writer::flow::SlotFlow as _;

use crate::network::listener::{
    conn,
    writer::{self, deadline::Slot, memory},
};

type Payload<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B, S> =
    Send<'a, 'p, 's, 'w, 'd, ID, W, C, B, memory::Body<'a, 'p, 's, 'e, 'd, ID, S, B>>;

type Header<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B> =
    Send<'a, 'p, 's, 'w, 'd, ID, W, C, B, memory::Buffer<'a, 'p, 's, 'e, 'd, ID, B>>;

type Bounded<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B> =
    Send<'a, 'p, 's, 'w, 'd, ID, W, C, B, memory::Bounded<'a, 'p, 's, 'e, 'd, ID, B>>;

pub(super) mod body;
pub(super) mod build;
pub(super) mod capacity;

pub(super) struct Send<'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B, M>
where
    B: data::raw::Payload<'d>,
{
    slot: Slot<'a, 'w, 'd, ID, W, conn::State<'p, 's, 'd, ID, C, B>>,
    flights: &'a flight::Slots<'d, ID>,
    pending: pending::Handle<'a, 'd, ID>,
    buffer: M,
    lease: app::Lease<'a, 'd>,
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B> Header<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    pub(in crate::network::listener::conn::write::context) fn try_draft<R, F, E>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        call: F,
    ) -> Result<write::Outcome<R>, error::Error<B, R, E>>
    where
        F: FnOnce(&mut C, &mut [u8]) -> Result<write::Draft<B, R>, E>,
    {
        self.try_header(driver, build::Buffer(call))
            .map_err(|error| match error {
                error::Error::Capacity(cause) => error::Error::Capacity(cause),
                error::Error::Build(build::Failure::Build(cause)) => error::Error::Build(cause),
                error::Error::Build(build::Failure::Check(cause))
                | error::Error::Mismatch(cause) => error::Error::Mismatch(cause),
            })
    }

    pub(in crate::network::listener::conn::write::context) fn try_header<H, R, E>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        call: H,
    ) -> Result<write::Outcome<R>, error::Error<B, R, E>>
    where
        H: build::Build<C, B, Receipt = R, Error = E>,
    {
        let Self {
            mut slot,
            flights: slots,
            pending: ready,
            buffer,

            lease,
        } = self;
        match buffer {
            memory::Buffer::Queued {
                slot: header,
                queue,
                plan,
            } => {
                let wake = slot.io().wake_target();
                let now = driver.driver_ref().clock().now();
                let vacant = match plan.poll_reserve(driver.region_token(), wake) {
                    task::Poll::Ready(Ok(vacant)) => vacant,
                    task::Poll::Ready(Err(cause)) => {
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Capacity(write::Reject::Geometry(
                            Full::Admission(cause),
                        )));
                    }
                    task::Poll::Pending => {
                        slot.ensure_send(now);
                        return Ok(write::Outcome::Deferred(write::Deferred::Capacity));
                    }
                };
                let commit = call.build(&mut slot.state.conn, header);
                let commit = match commit {
                    Ok(commit) => commit,
                    Err(cause) => {
                        vacant.cancel();
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Build(cause));
                    }
                };
                let (value, end, status) = match commit.try_publish(vacant) {
                    Ok(effect) => effect,
                    Err(cause) => {
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Mismatch(cause));
                    }
                };
                match status {
                    reserve::Publish::Published => lease.commit().run(|| {
                        Self::apply_end(&mut slot, end);
                        ready.wake();
                    }),
                    reserve::Publish::Empty => {
                        Self::apply_end(&mut slot, end);
                        slot.cancel_idle(queue.pending());
                    }
                }
                Ok(write::Outcome::Committed(value))
            }
            memory::Buffer::Direct(flight) => {
                let commit = call.build(&mut slot.state.conn, flight);
                let commit = match commit {
                    Ok(commit) => commit,
                    Err(cause) => {
                        slot.cancel_send();
                        return Err(error::Error::Build(cause));
                    }
                };
                let (value, end, direct) = commit.direct();
                match direct {
                    Some(direct) => {
                        slot.state.send.begin(direct);
                        slot.ensure_send(driver.driver_ref().clock().now());
                        slot.advance_send(slots, ready, driver);
                        lease.commit().run(|| {
                            Self::apply_end(&mut slot, end);
                            ready.wake();
                        });
                    }
                    None => {
                        slot.cancel_send();
                        Self::apply_end(&mut slot, end);
                    }
                }
                Ok(write::Outcome::Committed(value))
            }
        }
    }

    fn apply_end(
        slot: &mut types::Connection<'w, 'd, ID, W, conn::State<'p, 's, 'd, ID, C, B>>,
        end: write::End,
    ) {
        match end {
            write::End::Open => {}
            write::End::Shutdown => slot.shutdown(),
            write::End::Close => slot.close_after(),
        }
    }
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B> Bounded<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d> + 'd,
{
    pub(in crate::network::listener::conn::write::context) fn execute<R, H>(
        mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
        commit: H,
    ) -> Result<write::Outcome<R>, error::Error<B, R, Infallible>>
    where
        H: FnOnce(&mut C, &mut [u8]) -> write::Draft<B, R>,
    {
        let draft = match &mut self.buffer {
            memory::Bounded::Direct(flight) => commit(&mut self.slot.state.conn, flight.as_mut()),
            memory::Bounded::Queued { slot, .. } => {
                commit(&mut self.slot.state.conn, slot.as_mut())
            }
        };
        self.publish(driver, draft)
    }

    pub(in crate::network::listener::conn::write::context) fn try_prefix<R, H>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        call: H,
    ) -> Result<write::Outcome<R>, error::Error<B, R, Infallible>>
    where
        H: FnOnce(&mut C, &mut [u8]) -> write::Draft<Infallible, R>,
    {
        let Self {
            mut slot,
            flights,
            pending,
            buffer,
            lease,
        } = self;
        let buffer = match buffer {
            memory::Bounded::Direct(flight) => memory::Buffer::Direct(flight),
            memory::Bounded::Queued {
                slot: header,
                queue,
            } => {
                let plan = match queue.plan(reserve::Bounds::one(Limit::at_most::<
                    { write::Prefix::CAPACITY },
                    { write::Prefix::CAPACITY as u32 },
                >())) {
                    Ok(plan) => plan,
                    Err(cause) => {
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Capacity(write::Reject::Geometry(Full::Lane(
                            cause,
                        ))));
                    }
                };
                memory::Buffer::Queued {
                    slot: header,
                    queue,
                    plan,
                }
            }
        };
        Send {
            slot,
            flights,
            pending,
            buffer,
            lease,
        }
        .try_draft(driver, |state, bytes| Ok(call(state, bytes).into_empty()))
    }

    fn publish<R>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        draft: write::Draft<B, R>,
    ) -> Result<write::Outcome<R>, error::Error<B, R, Infallible>> {
        let Self {
            mut slot,
            flights,
            pending,
            buffer,
            lease,
        } = self;
        match buffer {
            memory::Bounded::Direct(flight) => {
                let valid = match modes::Bounded::try_new(flight, draft) {
                    Ok(valid) => valid.validate(),
                    Err(cause) => {
                        slot.cancel_send();
                        return Err(error::Error::Mismatch(cause));
                    }
                };
                let (receipt, end, direct) = valid.direct();
                match direct {
                    Some(direct) => {
                        slot.state.send.begin(direct);
                        slot.ensure_send(driver.driver_ref().clock().now());
                        slot.advance_send(flights, pending, driver);
                        lease.commit().run(|| {
                            Header::apply_end(&mut slot, end);
                            pending.wake();
                        });
                    }
                    None => {
                        slot.cancel_send();
                        Header::apply_end(&mut slot, end);
                    }
                }
                Ok(write::Outcome::Committed(receipt))
            }
            memory::Bounded::Queued {
                slot: header,
                queue,
            } => {
                let checked = match modes::Bounded::try_new(header, draft) {
                    Ok(checked) => checked,
                    Err(cause) => {
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Mismatch(cause));
                    }
                };
                let checked = match checked.split() {
                    ControlFlow::Break((receipt, end)) => {
                        Header::apply_end(&mut slot, end);
                        slot.cancel_idle(queue.pending());
                        return Ok(write::Outcome::Committed(receipt));
                    }
                    ControlFlow::Continue(pending) => pending,
                };
                let vacant = match checked.try_bounds() {
                    Ok(bounds) => match queue.plan(bounds) {
                        Ok(plan) => plan
                            .try_reserve(driver.region_token())
                            .map_err(|cause| error::Bounded::Geometry(Full::Admission(cause))),
                        Err(cause) => Err(error::Bounded::Geometry(Full::Lane(cause))),
                    },
                    Err(cause) => Err(cause),
                };
                let vacant = match vacant {
                    Ok(vacant) => vacant,
                    Err(cause) => {
                        let draft = checked.into_draft();
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Mismatch(error::Mismatch::bounded(
                            cause, draft,
                        )));
                    }
                };
                let (receipt, end, publish) = match checked.validate().try_publish(vacant) {
                    Ok(effect) => effect,
                    Err(cause) => {
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Mismatch(cause));
                    }
                };
                match publish {
                    reserve::Publish::Published => lease.commit().run(|| {
                        Header::apply_end(&mut slot, end);
                        pending.wake();
                    }),
                    reserve::Publish::Empty => {
                        Header::apply_end(&mut slot, end);
                        slot.cancel_idle(queue.pending());
                    }
                }
                Ok(write::Outcome::Committed(receipt))
            }
        }
    }
}
