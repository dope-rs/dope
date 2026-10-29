use conn::write;
use dope_core::driver::{
    route::Identity,
    schedule::{app, clock},
};
use dope_net::{
    link::egress::{
        self,
        data::{self, Bound},
    },
    wire,
};
use egress::reserve::error;
use o3::{base::cell::region, buffer::resident::bound};
use writer::{
    flow::{Flow::Clear, SlotFlow as _},
    memory::{self, Buffer, Claim},
};

use crate::network::listener::{
    conn::{
        self,
        write::context::send::{Bounded, Header, Payload, Send},
    },
    writer,
};

pub(in crate::network::listener::conn::write::context) enum Unavailable {
    Deferred(write::Deferred),
    Closed,
    Rejected(write::Reject),
}

enum Direct {
    Absent,
    Queued,
    Waiting,
}

pub(super) struct Unsent<S> {
    cause: Unavailable,
    value: egress::NonEmpty<S>,
}

impl<S> Unsent<S> {
    pub(super) fn into_parts(self) -> (Unavailable, egress::NonEmpty<S>) {
        (self.cause, self.value)
    }
}

pub(in crate::network::listener::conn::write::context) struct Capacity<
    'e,
    'a,
    'c,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B,
> where
    B: data::raw::Payload<'d>,
{
    scope: &'a mut conn::Ctx<'e, 'c, 'p, 's, 'w, 'd, ID, W, C, B>,
    now: clock::Moment<'d>,
    lease: app::Lease<'a, 'd>,
}

impl<'e, 'a, 'c, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B>
    Capacity<'e, 'a, 'c, 'p, 's, 'w, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    pub(in crate::network::listener::conn::write::context) const fn new(
        scope: &'a mut conn::Ctx<'e, 'c, 'p, 's, 'w, 'd, ID, W, C, B>,
        now: clock::Moment<'d>,
        lease: app::Lease<'a, 'd>,
    ) -> Self {
        Self { scope, now, lease }
    }

    pub(super) fn try_body<S>(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        value: egress::NonEmpty<S>,
    ) -> Result<Payload<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B, S>, Unsent<S>>
    where
        B: 'd,
    {
        let Self { scope, now, lease } = self;
        let slot = &mut scope.slot;
        if slot.send_status().is_closed() {
            return Err(Unsent {
                cause: Unavailable::Closed,
                value,
            });
        }
        let conn::Output::Open {
            slots,
            permit,
            held,
            queue,
        } = &mut scope.output
        else {
            return Err(Unsent {
                cause: Unavailable::Closed,
                value,
            });
        };
        let target = slot.io().wake_target();
        let flow = slot.flow(queue.pending());
        let (direct, _) = held.sources();
        let direct = if !matches!(&flow, Clear) {
            if direct.is_some() {
                Direct::Queued
            } else {
                Direct::Absent
            }
        } else {
            match direct {
                Some(source) => match held.take_direct(source, target) {
                    Claim::Acquired(flight) => {
                        queue.cancel_wait(token);
                        return Ok(super::Send {
                            slot: slot.reborrow(),
                            flights: slots,
                            pending: *permit,
                            buffer: memory::Body::Direct {
                                slot: flight,
                                value,
                            },

                            lease,
                        });
                    }
                    Claim::Waiting => Direct::Waiting,
                },
                None => Direct::Absent,
            }
        };
        let offer = match queue.offer(value) {
            Ok(offer) => offer,
            Err(error) => {
                let (value, reason) = error.into_parts();
                match direct {
                    Direct::Absent => {}
                    Direct::Queued => {
                        held.cancel(token, queue, target);
                        slot.ensure_send(now);
                        return Err(Unsent {
                            cause: Unavailable::Deferred(write::Deferred::OrderedDrain),
                            value,
                        });
                    }
                    Direct::Waiting => {
                        queue.cancel_wait(token);
                        slot.ensure_send(now);
                        return Err(Unsent {
                            cause: Unavailable::Deferred(write::Deferred::Capacity),
                            value,
                        });
                    }
                }
                held.cancel(token, queue, target);
                return Err(Unsent {
                    cause: Unavailable::Rejected(write::Reject::Geometry(error::Full::Reserve(
                        reason,
                    ))),
                    value,
                });
            }
        };
        held.cancel_wait(target);
        Ok(super::Send {
            slot: slot.reborrow(),
            flights: slots,
            pending: *permit,
            buffer: memory::Body::Queued { offer },

            lease,
        })
    }

    pub(in crate::network::listener::conn::write::context) fn try_bounded(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Bounded<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B>, Unavailable>
    where
        B: 'd,
    {
        let Self { scope, now, lease } = self;
        let slot = &mut scope.slot;
        if slot.send_status().is_closed() {
            return Err(Unavailable::Closed);
        }
        let conn::Output::Open {
            slots,
            permit,
            held,
            queue,
        } = &mut scope.output
        else {
            return Err(Unavailable::Closed);
        };
        let target = slot.io().wake_target();
        let flow = slot.flow(queue.pending());
        let (direct, header) = held.sources();
        if !matches!(&flow, Clear) && direct.is_some() && header.is_none() {
            held.cancel(token, queue, target);
            slot.ensure_send(now);
            return Err(Unavailable::Deferred(write::Deferred::OrderedDrain));
        }
        if let (Clear, Some(source)) = (&flow, direct) {
            match held.take_direct(source, target) {
                Claim::Acquired(flight) => {
                    queue.cancel_wait(token);
                    return Ok(Send {
                        slot: slot.reborrow(),
                        flights: slots,
                        pending: *permit,
                        buffer: memory::Bounded::Direct(flight),

                        lease,
                    });
                }
                Claim::Waiting if header.is_none() => {
                    queue.cancel_wait(token);
                    slot.ensure_send(now);
                    return Err(Unavailable::Deferred(write::Deferred::Capacity));
                }
                Claim::Waiting => {}
            }
        }
        let Some(header) = header else {
            held.cancel(token, queue, target);
            return Err(Unavailable::Rejected(write::Reject::Header));
        };
        let header = match held.take_header(header, target) {
            Claim::Acquired(header) => header,
            Claim::Waiting => {
                queue.cancel_wait(token);
                slot.ensure_send(now);
                return Err(Unavailable::Deferred(write::Deferred::Capacity));
            }
        };
        queue.cancel_wait(token);
        Ok(Send {
            slot: slot.reborrow(),
            flights: slots,
            pending: *permit,
            buffer: memory::Bounded::Queued {
                slot: header,
                queue: queue.reborrow(),
            },

            lease,
        })
    }
}

impl<'e, 'a, 'c, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B, G>
    Capacity<'e, 'a, 'c, 'p, 's, 'w, 'd, ID, W, C, Bound<B, G>>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
    G: bound::Bound,
{
    pub(in crate::network::listener::conn::write::context) fn take(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Header<'a, 'p, 's, 'w, 'e, 'd, ID, W, C, Bound<B, G>>, Unavailable> {
        let Self { scope, now, lease } = self;
        let slot = &mut scope.slot;
        if slot.send_status().is_closed() {
            return Err(Unavailable::Closed);
        }
        let conn::Output::Open {
            slots,
            permit,
            held,
            queue,
        } = &mut scope.output
        else {
            return Err(Unavailable::Closed);
        };
        let target = slot.io().wake_target();
        let flow = slot.flow(queue.pending());
        let (direct, header) = held.sources();
        let plan = header.zip(queue.prove());
        if !matches!(&flow, Clear) && direct.is_some() && plan.is_none() {
            held.cancel(token, queue, target);
            slot.ensure_send(now);
            return Err(Unavailable::Deferred(write::Deferred::OrderedDrain));
        } else if matches!(&flow, Clear)
            && let Some(source) = direct
        {
            match held.take_direct(source, target) {
                Claim::Acquired(flight) => {
                    queue.cancel_wait(token);
                    return Ok(super::Send {
                        slot: slot.reborrow(),
                        flights: slots,
                        pending: *permit,
                        buffer: Buffer::Direct(flight),

                        lease,
                    });
                }
                Claim::Waiting => {
                    if plan.is_none() {
                        queue.cancel_wait(token);
                        slot.ensure_send(now);
                        return Err(Unavailable::Deferred(write::Deferred::Capacity));
                    }
                }
            }
        }
        let Some((header, plan)) = plan else {
            held.cancel(token, queue, target);
            return Err(Unavailable::Rejected(write::Reject::Header));
        };
        let buffer = match held.take_header(header, target) {
            Claim::Acquired(slot) => Buffer::Queued {
                slot,
                queue: queue.reborrow(),
                plan,
            },
            Claim::Waiting => {
                queue.cancel_wait(token);
                slot.ensure_send(now);
                return Err(Unavailable::Deferred(write::Deferred::Capacity));
            }
        };
        Ok(super::Send {
            slot: slot.reborrow(),
            flights: slots,
            pending: *permit,
            buffer,

            lease,
        })
    }
}
