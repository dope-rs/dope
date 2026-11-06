use std::{net, num::NonZero};

use dope_core::{
    driver::{
        Context, result,
        retained::flight,
        route,
        route::{Identity, kind::Send, table},
        schedule,
        schedule::app,
    },
    io::{datagram, fd::handles, recv},
};
use o3::{
    alloc,
    base::cell::region,
    buffer::{
        self,
        bytes::bounded,
        pool,
        resident::{Budget, Charge},
    },
    depot::arena::slab::Vacancy,
    mem::quota,
};
use plan::bounds;
use send::{flights, kernel, owners, pending, retained};

use crate::network::datagram::{RejectCause, Rejected, plan, send};

pub(in crate::network::datagram) struct Sender<
    'd,
    ID: Identity,
    B = pool::Cursor,
    M: send::Mode = send::Plain,
    F = flight::Slots<'d, <ID as route::Materialize<Send>>::With>,
> {
    pub(super) pending: pending::Pending<'d, ID, B, M>,
    pub(super) in_flight:
        table::Slab<flights::Backing<'d, B, M>, <ID as route::Materialize<Send>>::With>,
    budget: Option<Budget<'d>>,
    receive_slot: NonZero<usize>,
    flights: F,
    pub(super) space: route::Space<'d, <ID as route::Materialize<Send>>::With>,
    pub(super) phase: send::Phase,
}

impl<ID: Identity, B, M: send::Mode, F> Sender<'_, ID, B, M, F> {
    pub(in crate::network::datagram) fn next_generation(&self) -> Option<route::Epoch> {
        self.pending
            .next_generation()
            .zip(self.in_flight.view().next_generation())
            .map(|(queued, flight)| queued.max(flight))
    }
}

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    Sender<'d, ID, B, M>
{
    pub(in crate::network::datagram) fn try_new(
        queue: bounds::Pending,
        budget: Option<Budget<'d>>,
        slots: bounds::Flights,
        bytes: NonZero<usize>,
        space: route::Space<'d, <ID as route::Materialize<Send>>::With>,
    ) -> Result<Sender<'d, ID, B, M, ()>, alloc::Error> {
        Ok(Sender {
            pending: pending::Pending::try_new(queue.into_table())?,
            in_flight: table::Slab::try_new(slots.into_table())?,
            budget,
            receive_slot: bytes,
            flights: (),
            space,
            phase: send::Phase::Ready,
        })
    }

    pub(in crate::network::datagram) fn is_accepting(&self) -> bool {
        self.phase.can_accept()
    }

    pub(super) fn try_enqueue<P, F>(
        &mut self,
        value: P,
        addr: M::Address,
        build: F,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        M: send::Segmented,
        P: buffer::Residency + buffer::raw::Source,
        F: FnOnce(bounded::Value<P>) -> kernel::Payload<'d, B>,
    {
        self.try_queue(value, M::address(&addr), M::control(addr, None), build)
    }

    pub(in crate::network::datagram) fn try_queue<P, F>(
        &mut self,
        value: P,
        addr: net::SocketAddr,
        control: M::Queued,
        build: F,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        P: buffer::Residency + buffer::raw::Source,
        F: FnOnce(bounded::Value<P>) -> kernel::Payload<'d, B>,
    {
        self.enqueue(value, addr, resident, |value| {
            let value = try_bound(value)?;
            Ok((build(value), control))
        })
    }

    pub(super) fn gso<P, T>(
        &mut self,
        value: P,
        addr: M::Address,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        M: send::Segmented,
        P: buffer::Residency + buffer::raw::Convert<datagram::GsoPayload<T>>,
        T: buffer::raw::Convert<Vec<u8>>,
    {
        self.enqueue(value, M::address(&addr), resident, |value| {
            let value = buffer::raw::Convert::into(value);
            // SAFETY: conversion preserves the admitted extent, and the queued
            // owner retains its exact payload and segment together through retirement.
            let (payload, segment) = unsafe { datagram::raw::Payload::from(value).into_parts() };
            Ok((
                kernel::Payload::Owned(payload.into_storage()),
                M::control(addr, Some(segment)),
            ))
        })
    }

    fn enqueue<P, F>(
        &mut self,
        value: P,
        addr: net::SocketAddr,
        resident: impl FnOnce(&P) -> Option<NonZero<usize>>,
        build: F,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        F: FnOnce(P) -> Result<(kernel::Payload<'d, B>, M::Queued), Rejected<P>>,
    {
        if let Some(cause) = self.phase.rejection() {
            return Err(Rejected::new(value, cause));
        }
        let Self {
            pending: queue,
            budget,
            ..
        } = self;
        let entry = match queue.back() {
            Ok(entry) => entry,
            Err(cause) => return Err(Rejected::new(value, cause)),
        };
        let charge = match resident(&value)
            .map(|size| try_charge(*budget, size))
            .transpose()
        {
            Ok(charge) => charge,
            Err(error) => return Err(Rejected::new(value, error)),
        };
        let (value, control) = build(value)?;
        let hold = retained::Retained::new(value, charge);
        Ok(entry.push(send::Queued::new(hold, addr, control)))
    }

    pub(in crate::network::datagram) fn try_receive(
        &mut self,
        value: recv::Lease<'d>,
        addr: net::SocketAddr,
        control: M::Queued,
    ) -> Result<(), Rejected<recv::Lease<'d>>> {
        let size = self.receive_slot;
        self.enqueue(
            value,
            addr,
            |_| Some(size),
            |value| Ok((kernel::Payload::Packet(value), control)),
        )?;
        Ok(())
    }

    pub(in crate::network::datagram) fn progress(
        &self,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        let live = self.in_flight.view();
        if self.pending.is_empty() {
            return if live.is_empty() {
                schedule::Progress::Quiescent
            } else {
                schedule::Progress::waiting(region)
            };
        }
        if !self.phase.can_accept()
            || (self.phase == send::Phase::Ready && live.len() < live.capacity().count())
        {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::waiting(region)
        }
    }

    pub(in crate::network::datagram) fn stop(&mut self) {
        self.phase.stop();
    }

    pub(in crate::network::datagram) fn fail(&mut self, error: result::SubmitError) {
        if self.phase.can_accept() {
            self.phase = send::Phase::Failed(error);
        }
    }

    pub(in crate::network::datagram) fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.in_flight.view().is_empty()
    }
}

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    Sender<'d, ID, B, M, ()>
{
    pub(in crate::network::datagram) fn commit(
        mut self,
        flights: flight::Slots<'d, <ID as route::Materialize<Send>>::With>,
        floor: Option<route::Epoch>,
    ) -> Sender<'d, ID, B, M> {
        self.pending.advance_to(floor);
        self.in_flight.access().advance_to(floor);
        let Self {
            pending,
            in_flight: active,
            budget,
            receive_slot: bytes,
            flights: (),
            space,
            phase,
        } = self;
        Sender {
            pending,
            in_flight: active,
            budget,
            receive_slot: bytes,
            flights,
            space,
            phase,
        }
    }
}

fn resident<P: buffer::Residency>(value: &P) -> Option<NonZero<usize>> {
    NonZero::new(value.resident())
}

fn try_bound<P: buffer::raw::Source>(value: P) -> Result<bounded::Value<P>, Rejected<P>> {
    bounded::Value::try_new(value).map_err(|rejected| {
        let (value, error) = rejected.into_parts();
        Rejected::new(value, RejectCause::Transfer(error))
    })
}

fn try_charge(budget: Option<Budget<'_>>, size: NonZero<usize>) -> Result<Charge<'_>, RejectCause> {
    let budget = budget.ok_or(RejectCause::BudgetMissing {
        required: size.get(),
    })?;
    budget.try_charge(size.get()).map_err(|error| {
        let (required, free) = error.into_parts();
        RejectCause::Resident {
            required,
            available: free,
        }
    })
}

mod raw {
    use dope_core::driver::retained::raw::Submission;

    use super::{
        Context, Identity, Send, Sender, Vacancy, app, buffer, flights, handles, owners, pending,
        quota, result, send,
    };

    impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
        Sender<'d, ID, B, M>
    {
        pub(in crate::network::datagram) fn flush_with<'fd, 'turn>(
            &mut self,
            mut select: impl FnMut(&M::Queued) -> &'fd handles::Datagram<'d>,
            work: app::Work<'turn, 'd>,
            driver: &mut Context<'_, 'd>,
            mut transfer: impl FnMut(send::Transfer<'d, ID>),
        ) -> Result<(), result::SubmitError>
        where
            'd: 'fd,
        {
            let Self {
                pending: queue,
                in_flight: slots,
                flights: ops,
                space,
                phase,
                ..
            } = self;
            match phase {
                send::Phase::Ready => {}
                send::Phase::Deferred => *phase = send::Phase::Ready,
                send::Phase::Failed(_) | send::Phase::Stopped => return Ok(()),
            }
            loop {
                let step = work.run(|| {
                    let front = queue.front()?;
                    let slot = match slots.vacancy().try_reserve() {
                        Ok(slot) => slot,
                        Err(Vacancy::Full) => return None,
                        Err(Vacancy::IdentityExhausted) => {
                            let error = result::SubmitError::from(Vacancy::IdentityExhausted);
                            *phase = send::Phase::Failed(error);
                            return Some(Err(error));
                        }
                    };
                    let key = slot.key();
                    let target = (*space).bind(key.slot(), key.epoch());
                    let moved = front.submit(|queued| {
                        let fd = select(&queued.control);
                        let mut slot = slot.stage(flights::Backing::new(queued));
                        let (message, flight) = slot.get_mut().fill_message();
                        let op = Submission::send_msg(fd, message, target);
                        // SAFETY: the reserved slab retains the complete message graph
                        // at stable addresses through completion or quiescent finish.
                        match unsafe { op.try_submit(driver.reborrow(), ops) } {
                            Ok(op) => {
                                // This field was split before submission; writing it
                                // cannot reborrow the graph now retained by the driver.
                                *flight = Some(op);
                                slot.publish();
                                Ok(owners::Flight::new(target.operation::<Send>()))
                            }
                            Err(error) => Err((error, slot.reject().into_queued())),
                        }
                    });
                    Some(match moved {
                        pending::Commit::Submitted((queued, flight)) => {
                            transfer(send::Transfer::new(queued, flight));
                            Ok(true)
                        }
                        pending::Commit::Deferred(error) => match error.cause() {
                            result::SubmitCause::Full => {
                                *phase = send::Phase::Deferred;
                                Ok(false)
                            }
                            result::SubmitCause::IdentityExhausted => {
                                *phase = send::Phase::Failed(error);
                                Err(error)
                            }
                        },
                    })
                });
                match step {
                    quota::Admission::Item(result) => {
                        if !result? {
                            break;
                        }
                    }
                    quota::Admission::Empty | quota::Admission::Exhausted => break,
                }
            }
            Ok(())
        }
    }
}
