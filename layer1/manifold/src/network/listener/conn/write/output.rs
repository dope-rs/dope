use std::{marker, mem::MaybeUninit, task};

use dope_core::driver::{self, route::Identity, schedule::app};
use dope_net::{link::egress, wire};
use egress::{data, load::Value, reserve};
use o3::{
    buffer::{resident::bound, write::Slice},
    mem::{Lease, Return, lease::Open, slot},
};

use crate::network::listener::{
    conn::{self, write},
    writer::{self, deadline::Slot, memory},
};

mod raw;

/// One admitted native output, retained until publication or abandonment.
#[must_use = "dropping an output releases its unpublished reservation"]
pub struct Output<
    'a,
    'c,
    'p,
    's,
    'w,
    'e,
    'io,
    'd: 'w + 'e,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B: data::raw::Payload<'d>,
    Q: reserve::Geometry = reserve::Bounds,
    G: bound::Bound = bound::AtMost<0>,
> {
    lease: Lease<Owner<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, Q, G>, Open>,
    written: usize,
}

pub(super) struct Owner<
    'a,
    'c,
    'p,
    's,
    'w,
    'e,
    'io,
    'd: 'w + 'e,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B: data::raw::Payload<'d>,
    Q: reserve::Geometry,
    G: bound::Bound,
> {
    pub(super) slot: &'a mut Slot<'c, 'p, 's, 'w, 'd, ID, W, C, B>,
    pub(super) open: &'a conn::Open<'c, 'p, 's, 'e, 'd, ID, B>,
    pub(super) work: app::Lease<'a, 'd>,
    pub(super) scope: marker::PhantomData<(&'a &'e (), &'a mut driver::Context<'io, 'd>, fn() -> (Q, G))>,
}

pub(super) enum Reservation<
    'a,
    'p,
    's,
    'e,
    'io,
    'd: 'e,
    ID: Identity,
    B: data::raw::Payload<'d>,
    Q: reserve::Geometry,
    G: bound::Bound,
> where
    Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>: 'a,
{
    Direct {
        header: memory::raw::DirectSlot<'p, 's, 'd, ID, B>,
        driver: &'a mut driver::Context<'io, 'd>,
    },
    Queued(G::Stored<'a, Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>>),
}

pub(super) struct Queued<'a, 'p, 's, 'e, 'd: 'e, ID: Identity, B: data::raw::Payload<'d>, Q: reserve::Geometry> {
    header: memory::HeaderSlot<'p, 's, 'd, ID>,
    vacant: reserve::Vacant<
        'a,
        'a,
        'e,
        'd,
        writer::Payload<'p, 's, 'd, ID, B>,
        writer::Reserve<'d, B>,
        Q,
    >,
    pending: egress::Pending,
}

impl<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd: 'w + 'e, ID: Identity, W, C, B, Q: reserve::Geometry, G: bound::Bound> Return
    for Owner<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, Q, G>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    type Item = Reservation<'a, 'p, 's, 'e, 'io, 'd, ID, B, Q, G>;

    fn return_item(self, item: Self::Item) {
        match item {
            Reservation::Direct { header, .. } => {
                drop(header);
                self.slot.cancel_send();
            }
            Reservation::Queued(queued) => {
                let Queued {
                    header,
                    vacant,
                    pending,
                } = G::into_inner::<Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>>(queued);
                drop(header);
                drop(vacant);
                self.slot.cancel_idle(pending);
            }
        }
    }
}

impl<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd: 'w + 'e, ID: Identity, W, C, B, Q: reserve::Geometry, G: bound::Bound>
    Output<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, Q, G>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    pub(super) fn insert<'out>(
        slot: &'a mut Slot<'c, 'p, 's, 'w, 'd, ID, W, C, B>,
        open: &'a conn::Open<'c, 'p, 's, 'e, 'd, ID, B>,
        work: app::Lease<'a, 'd>,
        reservation: Reservation<'a, 'p, 's, 'e, 'io, 'd, ID, B, Q, G>,
        output: &'out mut MaybeUninit<Self>,
    ) -> slot::Occupied<'out, Self> {
        slot::Vacant::new(output).insert(Self {
            lease: Lease::open(
                Owner {
                    slot,
                    open,
                    work,
                    scope: marker::PhantomData,
                },
                reservation,
            ),
            written: 0,
        })
    }

    pub(super) fn prepare<'out, P>(
        connection: &'a mut conn::Ctx<'e, 'c, 'p, 's, 'w, 'd, ID, W, C, B>,
        driver: &'a mut driver::Context<'io, 'd>,
        output: &'out mut MaybeUninit<Self>,
        prepare: impl FnOnce(
            &egress::Queue<
                'c,
                'e,
                'd,
                { crate::network::listener::IOV_CAP },
                writer::Payload<'p, 's, 'd, ID, B>,
                writer::Reserve<'d, B>,
            >,
        ) -> Option<(G::Finite, P)>,
        plan: impl FnOnce(
            &egress::Queue<
                'c,
                'e,
                'd,
                { crate::network::listener::IOV_CAP },
                writer::Payload<'p, 's, 'd, ID, B>,
                writer::Reserve<'d, B>,
            >,
            P,
        ) -> Result<memory::Plan<'c, 'p, 's, 'e, 'd, ID, B, Q>, reserve::error::Lane>,
    ) -> Result<slot::Occupied<'out, Self>, write::error::Admission> {
        use memory::Claim;
        use write::error::Admission;

        let Some(work) = connection.work.lease() else {
            connection.slot.io().wake_target().wake();
            return Err(Admission::Deferred(write::Deferred::Turn));
        };
        let slot = &mut connection.slot;
        let Some(open) = connection.output else {
            return Err(Admission::Closed);
        };
        if slot.send_status().is_closed() {
            return Err(Admission::Closed);
        }
        let queue = &open.queue;
        let target = slot.io().wake_target();
        let pending = queue.pending();
        let (slot, direct) = match slot.direct(&open.held, pending, target) {
            Ok(granted) => {
                queue.cancel_wait(driver.region_token());
                return Ok(granted.output(open, work, driver, output));
            }
            Err(other) => other,
        };
        let prepared = open.held.header().zip(prepare(queue));
        let Some((header, (proof, prepared))) = prepared else {
            let cause = match direct {
                write::direct::Deferred::Absent => {
                    open.held.cancel(driver.region_token(), queue, target);
                    return Err(Admission::Rejected(write::Reject::Header));
                }
                write::direct::Deferred::Ordered => {
                    open.held.cancel(driver.region_token(), queue, target);
                    write::Deferred::OrderedDrain
                }
                write::direct::Deferred::Capacity => {
                    queue.cancel_wait(driver.region_token());
                    write::Deferred::Capacity
                }
            };
            slot.ensure_send(driver.driver_ref().clock().now());
            return Err(Admission::Deferred(cause));
        };
        let header = match open.held.take_header(header, target) {
            Claim::Acquired(header) => header,
            Claim::Waiting => {
                queue.cancel_wait(driver.region_token());
                slot.ensure_send(driver.driver_ref().clock().now());
                return Err(Admission::Deferred(write::Deferred::Capacity));
            }
        };
        let plan = match plan(queue, prepared) {
            Ok(plan) => plan,
            Err(cause) => {
                drop(header);
                queue.cancel_wait(driver.region_token());
                slot.cancel_idle(pending);
                return Err(Admission::Rejected(write::Reject::Geometry(
                    reserve::error::Full::Lane(cause),
                )));
            }
        };
        let now = driver.driver_ref().clock().now();
        let (vacant, acquire) = plan.prepare(driver.region_token());
        let mut output = Self::insert(
            slot,
            open,
            work,
            Reservation::Queued(G::store(
                proof,
                Queued {
                    header,
                    vacant,
                    pending,
                },
            )),
            output,
        );
        let (_, reservation) = output.get_mut().lease.parts_mut();
        let Reservation::Queued(queued) = reservation else {
            unreachable!("output");
        };
        let queued = G::get_mut::<Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>>(queued);
        match acquire.poll_reserve(&mut queued.vacant, target) {
            task::Poll::Ready(Ok(())) => Ok(output),
            task::Poll::Ready(Err(cause)) => {
                drop(output);
                Err(Admission::Rejected(write::Reject::Geometry(
                    reserve::error::Full::Admission(cause),
                )))
            }
            task::Poll::Pending => {
                Self::defer(output, now);
                Err(Admission::Deferred(write::Deferred::Capacity))
            }
        }
    }

    /// Lends the connection state and the retained header's initialized prefix.
    pub fn parts(&mut self) -> (&mut C, Slice<'_, &mut usize>) {
        let (owner, reservation) = self.lease.parts_mut();
        let bytes = match reservation {
            Reservation::Direct { header, .. } => header.as_mut(),
            Reservation::Queued(queued) => {
                G::get_mut::<Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>>(queued)
                    .header
                    .as_mut()
            }
        };
        (owner.slot.conn, raw::borrow(bytes, &mut self.written))
    }

    /// Publishes the initialized header without a body.
    pub fn finish(
        output: slot::Occupied<'_, Self>,
        end: write::End,
    ) -> Result<(), write::error::Mismatch<B>> {
        Self::publish(output, (), end)
    }
}

impl<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd: 'w + 'e, ID: Identity, W, C, B, G: bound::Bound>
    Output<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, reserve::Bounds, G>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    pub fn try_publish(
        output: slot::Occupied<'_, Self>,
        body: Value<B>,
        end: write::End,
    ) -> Result<(), write::error::Mismatch<B>> {
        let body = match body {
            Value::Unchecked(body) => match body.try_nonempty() {
                Ok(body) => body,
                Err(rejected) => match egress::Empty::try_from(rejected) {
                    Ok(empty) => {
                        drop(empty);
                        return Self::finish(output, end);
                    }
                    Err(rejected) => {
                        let (body, cause) = rejected.into_parts();
                        return Err(write::error::Mismatch::Bounded {
                            cause: write::error::Bounded::Payload(cause),
                            written: output.written,
                            body: Some(Value::Unchecked(body)),
                            end,
                        });
                    }
                },
            },
            Value::Checked(body) => body,
        };
        Self::publish(output, body, end)
    }
}

fn apply_end<'d: 'w, 'w, ID: Identity, W, C, B>(
    slot: &mut Slot<'_, '_, '_, 'w, 'd, ID, W, C, B>,
    end: write::End,
) where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    match end {
        write::End::Open => {}
        write::End::Shutdown => slot.shutdown(),
        write::End::Close => slot.close_after(),
    }
}
