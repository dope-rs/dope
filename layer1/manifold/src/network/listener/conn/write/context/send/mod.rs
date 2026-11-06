use std::{convert::Infallible, ops::ControlFlow, task};

use conn::write;
use dope_core::driver::{self, retained::flight, route::Identity, schedule::app};
use dope_net::{
    link::{egress, pool::pending, slot::types},
    wire,
};
use egress::{
    data,
    reserve::{self, error::Full, limit::Limit},
};
use o3::base::cell::region;
use write::error;

use crate::network::listener::{
    conn,
    writer::{deadline::Slot, memory},
};

type Bounded<'a, 'c, 'p, 's, 'w, 'e, 'd, ID, W, C, B> =
    Header<'a, 'c, 'p, 's, 'w, 'e, 'd, ID, W, C, B, ()>;

pub(super) mod body;
pub(super) mod capacity;

pub(super) struct Header<
    'a,
    'c,
    'p,
    's,
    'w,
    'e,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B,
    P = memory::Plan<'a, 'p, 's, 'e, 'd, ID, B>,
> where
    B: data::raw::Payload<'d>,
{
    pub(super) slot: &'a mut Slot<'c, 'p, 's, 'w, 'd, ID, W, C, B>,
    pub(super) flights: &'a flight::Slots<'d, ID>,
    pub(super) pending: pending::Handle<'a, 'd, ID>,
    pub(super) buffer: memory::Buffer<'a, 'p, 's, 'e, 'd, ID, B, P>,
    pub(super) lease: app::Lease<'a, 'd>,
}

impl<'e, 'a, 'c, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B>
    Header<'a, 'c, 'p, 's, 'w, 'e, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    pub(in crate::network::listener::conn::write::context) fn try_draft<R, F, E>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        call: F,
    ) -> Result<write::Outcome<R>, error::Error<B, R, E>>
    where
        F: FnOnce(&mut C, &mut [u8]) -> Result<write::Draft<B, R>, E>,
    {
        self.try_header(driver, |state, bytes, owner| {
            let draft = call(state, bytes).map_err(error::Error::Build)?;
            write::Commit::try_draft(owner, draft).map_err(error::Error::Mismatch)
        })
    }

    pub(in crate::network::listener::conn::write::context) fn try_header<H, R, E>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        call: H,
    ) -> Result<write::Outcome<R>, error::Error<B, R, E>>
    where
        H: for<'b, 'id> FnOnce(
            &mut C,
            &'b mut [u8; write::Prefix::CAPACITY],
            region::Proof<region::Identity<'id>>,
        ) -> Result<
            write::Commit<region::Proof<region::Identity<'id>>, B, R>,
            error::Error<B, R, E>,
        >,
    {
        let Self {
            slot,
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
                let commit = write::Commit::try_with(&mut *slot.conn, header, call);
                let commit = match commit {
                    Ok(commit) => commit,
                    Err(cause) => {
                        vacant.cancel();
                        slot.cancel_idle(queue.pending());
                        return Err(cause);
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
                        apply_end(slot, end);
                        ready.wake();
                    }),
                    reserve::Publish::Empty => {
                        apply_end(slot, end);
                        slot.cancel_idle(queue.pending());
                    }
                }
                Ok(write::Outcome::Committed(value))
            }
            memory::Buffer::Direct(flight) => {
                let commit = write::Commit::try_with(&mut *slot.conn, flight, call);
                let commit = match commit {
                    Ok(commit) => commit,
                    Err(cause) => {
                        slot.cancel_send();
                        return Err(cause);
                    }
                };
                let (value, end, direct) = commit.direct();
                match direct {
                    Some(direct) => {
                        slot.state.send.begin(direct);
                        slot.ensure_send(driver.driver_ref().clock().now());
                        slot.advance_send(slots, ready, driver);
                        lease.commit().run(|| {
                            apply_end(slot, end);
                            ready.wake();
                        });
                    }
                    None => {
                        slot.cancel_send();
                        apply_end(slot, end);
                    }
                }
                Ok(write::Outcome::Committed(value))
            }
        }
    }
}

impl<'e, 'a, 'c, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B>
    Bounded<'a, 'c, 'p, 's, 'w, 'e, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d> + 'd,
{
    pub(in crate::network::listener::conn::write::context) fn execute<R, H>(
        mut self,
        driver: &mut driver::Context<'_, 'd>,
        commit: H,
    ) -> Result<write::Outcome<R>, error::Error<B, R, Infallible>>
    where
        H: FnOnce(&mut C, &mut [u8]) -> write::Draft<B, R>,
    {
        let draft = match &mut self.buffer {
            memory::Bounded::Direct(flight) => commit(&mut *self.slot.conn, flight.as_mut()),
            memory::Bounded::Queued { slot, .. } => commit(&mut *self.slot.conn, slot.as_mut()),
        };
        self.publish(driver, draft)
    }

    pub(in crate::network::listener::conn::write::context) fn try_prefix<R, H>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        call: H,
    ) -> Result<write::Outcome<R>, error::Error<B, R, Infallible>>
    where
        H: FnOnce(&mut C, &mut [u8]) -> write::Draft<Infallible, R>,
    {
        let Self {
            slot,
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
                ..
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
        Header {
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
        driver: &mut driver::Context<'_, 'd>,
        draft: write::Draft<B, R>,
    ) -> Result<write::Outcome<R>, error::Error<B, R, Infallible>> {
        let Self {
            slot,
            flights,
            pending,
            buffer,
            lease,
        } = self;
        match buffer {
            memory::Bounded::Direct(flight) => {
                let valid = match write::Commit::try_draft(flight, draft) {
                    Ok(valid) => valid,
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
                            apply_end(slot, end);
                            pending.wake();
                        });
                    }
                    None => {
                        slot.cancel_send();
                        apply_end(slot, end);
                    }
                }
                Ok(write::Outcome::Committed(receipt))
            }
            memory::Bounded::Queued {
                slot: header,
                queue,
                ..
            } => {
                let checked = match write::Commit::try_draft(header, draft) {
                    Ok(checked) => checked,
                    Err(cause) => {
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Mismatch(cause));
                    }
                };
                let checked = match checked.split() {
                    ControlFlow::Break((receipt, end)) => {
                        apply_end(slot, end);
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
                let (receipt, end, publish) = match checked.try_publish(vacant) {
                    Ok(effect) => effect,
                    Err(cause) => {
                        slot.cancel_idle(queue.pending());
                        return Err(error::Error::Mismatch(cause));
                    }
                };
                match publish {
                    reserve::Publish::Published => lease.commit().run(|| {
                        apply_end(slot, end);
                        pending.wake();
                    }),
                    reserve::Publish::Empty => {
                        apply_end(slot, end);
                        slot.cancel_idle(queue.pending());
                    }
                }
                Ok(write::Outcome::Committed(receipt))
            }
        }
    }
}

fn apply_end<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w>(
    slot: &mut types::Control<'w, 'd, ID, W>,
    end: write::End,
) {
    match end {
        write::End::Open => {}
        write::End::Shutdown => slot.shutdown(),
        write::End::Close => slot.close_after(),
    }
}
