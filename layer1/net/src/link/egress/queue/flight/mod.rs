mod entry;
use std::{mem, ops};

use dope_core::{
    driver::route::{self, kind},
    io::{socket::msg, transfer, transfer::progress},
};
use entry::Buffer;
pub(in crate::link::egress) use entry::{Entry, Lease};
use o3::{base::cell::region, buffer::resident, depot::arena::pool::recycle};

use crate::{
    link::egress::{
        self, data,
        load::credit,
        metadata,
        metadata::{arena, front},
        queue::{self, flight, prepared},
        reserve::demand,
    },
    wire::send,
};

pub(in crate::link::egress) struct Flight<'a, 'p, 'd, const IOV: usize> {
    lease: &'a mut Option<Lease<'p, 'd, IOV>>,
    flights: &'p recycle::Store<Entry<'d, IOV>>,
}

pub(in crate::link::egress) enum Completion<'a, 'p, 'd, const IOV: usize> {
    Stale,
    Invalid(egress::Invalid),
    Ready(Released<'a, 'p, 'd, IOV>),
}

impl<'a, 'p, 'd, const IOV: usize> Flight<'a, 'p, 'd, IOV> {
    pub(in crate::link::egress) fn new(
        lease: &'a mut Option<Lease<'p, 'd, IOV>>,
        flights: &'p recycle::Store<Entry<'d, IOV>>,
    ) -> Self {
        Self { lease, flights }
    }

    pub(in crate::link::egress) fn reborrow(&mut self) -> Flight<'_, 'p, 'd, IOV> {
        Flight::new(self.lease, self.flights)
    }

    pub(in crate::link::egress) fn take(self) -> Option<Active<'a, 'p, 'd, IOV>> {
        let lease = self.lease.take()?;
        Some(Active {
            slot: self.lease,
            lease,
        })
    }

    pub(in crate::link::egress) fn complete(
        &mut self,
        target: route::Erased<'d, kind::Send>,
        bytes: transfer::Len,
    ) -> Completion<'_, 'p, 'd, IOV> {
        let Some(active) = self.reborrow().take() else {
            return Completion::Stale;
        };
        if !active.matches(target) {
            active.restore();
            return Completion::Stale;
        }
        let maximum = active.lease.get().len();
        if bytes > maximum {
            active.restore();
            return Completion::Invalid(egress::Invalid::Excess {
                actual: bytes,
                maximum,
            });
        }
        Completion::Ready(Released { active, bytes })
    }
}

#[must_use]
pub(in crate::link::egress) struct Prepared<'a, 'p, 'd, const IOV: usize> {
    lease: &'a mut Option<Lease<'p, 'd, IOV>>,
    pub(super) reservation: Lease<'p, 'd, IOV>,
}

pub(in crate::link::egress) struct Active<'a, 'p, 'd, const IOV: usize> {
    slot: &'a mut Option<Lease<'p, 'd, IOV>>,
    lease: Lease<'p, 'd, IOV>,
}

impl<'a, 'p, 'd, const IOV: usize> Prepared<'a, 'p, 'd, IOV> {
    pub(in crate::link::egress) fn retain(mut self, target: route::Erased<'d, kind::Send>) {
        self.reservation.get_mut().mark(target);
        *self.lease = Some(self.reservation);
    }

    pub(in crate::link::egress) fn vectored(&mut self) -> send::Vectored<'_, IOV> {
        self.reservation.get_mut().vectored()
    }

    pub(in crate::link::egress) fn release(
        self,
        bytes: transfer::Len,
    ) -> Result<Released<'a, 'p, 'd, IOV>, egress::Invalid> {
        let maximum = self.reservation.get().len();
        if bytes > maximum {
            return Err(egress::Invalid::Excess {
                actual: bytes,
                maximum,
            });
        }
        Ok(Released {
            active: Active {
                slot: self.lease,
                lease: self.reservation,
            },
            bytes,
        })
    }
}

impl<'a, 'p, 'd, const IOV: usize> Active<'a, 'p, 'd, IOV> {
    pub(in crate::link::egress) fn matches(&self, target: route::Erased<'d, kind::Send>) -> bool {
        self.lease.get().matches(target)
    }

    pub(in crate::link::egress) fn restore(self) {
        *self.slot = Some(self.lease);
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Lease<'static, 'static, 32>>() == 3 * word);
    assert!(mem::size_of::<Option<Lease<'static, 'static, 32>>>() == 3 * word);
    assert!(mem::size_of::<Flight<'static, 'static, 'static, 32>>() == 2 * word);
    assert!(mem::size_of::<Released<'static, 'static, 'static, 32>>() == 5 * word);
    assert!(mem::size_of::<Entry<'static, 32>>() <= 768);
    assert!(
        mem::size_of::<
            Plan<
                'static,
                'static,
                'static,
                'static,
                32,
                resident::Shared<'static>,
                demand::Fallible,
            >,
        >() == 5 * word
    );
    assert!(
        mem::size_of::<
            prepared::Preparation<
                'static,
                'static,
                'static,
                'static,
                32,
                resident::Shared<'static>,
                demand::Fallible,
            >,
        >() == 5 * word
    );
};

/// One lane's source queue and its exclusively borrowed send reservation.
pub(in crate::link) struct Output<
    'a,
    'p,
    'd,
    const IOV: usize,
    B,
    D: demand::Mode = demand::Fallible,
> {
    queue: egress::Queue<'a, 'p, 'd, IOV, B, D>,
    flight: Flight<'a, 'p, 'd, IOV>,
}

impl<'a, 'p, 'd, const IOV: usize, B, D: demand::Mode> Output<'a, 'p, 'd, IOV, B, D> {
    pub(in crate::link::egress) fn new(
        slot: arena::MutableSlot<
            'a,
            'p,
            'd,
            B,
            Option<Lease<'p, 'd, IOV>>,
            credit::Ready<'p, 'd>,
            D,
        >,
        flights: &'p recycle::Store<Entry<'d, IOV>>,
    ) -> Self {
        let (lane, entries) = slot.into_parts();
        Self {
            queue: egress::Queue { entries },
            flight: Flight::new(lane, flights),
        }
    }

    pub(in crate::link) fn is_active(&self) -> bool {
        self.flight.lease.is_some()
    }
    pub(in crate::link) fn abort(&mut self, target: route::Erased<'d, kind::Send>) {
        let Some(active) = self.flight.reborrow().take() else {
            return;
        };
        if !active.matches(target) {
            active.restore();
        }
    }
}

impl<'a, 'p, 'd, const IOV: usize, B, D: demand::Mode> ops::Deref
    for Output<'a, 'p, 'd, IOV, B, D>
{
    type Target = egress::Queue<'a, 'p, 'd, IOV, B, D>;

    fn deref(&self) -> &Self::Target {
        &self.queue
    }
}
impl<'a, 'p, 'd, const IOV: usize, B: data::raw::Payload<'d>, D: demand::Mode>
    Output<'a, 'p, 'd, IOV, B, D>
{
    pub(in crate::link) fn prepare<'flight>(
        &'flight mut self,
    ) -> prepared::Preparation<'flight, 'a, 'p, 'd, IOV, B, D> {
        let Some(plan) = Plan::new(self) else {
            return prepared::Preparation::Empty;
        };
        prepared::Preparation::Ready(plan)
    }

    pub(in crate::link) fn complete(
        &mut self,
        token: &mut region::Token<region::Identity<'d>>,
        target: route::Erased<'d, kind::Send>,
        bytes: transfer::Len,
        release: impl FnMut(B),
    ) -> queue::Change {
        let released = match self.release(target, bytes) {
            Ok(released) => released,
            Err(change) => return change,
        };
        released.consume(token, release);
        queue::Change::Applied
    }

    fn release(
        &mut self,
        target: route::Erased<'d, kind::Send>,
        bytes: transfer::Len,
    ) -> Result<queue::Released<'_, 'a, 'p, 'd, IOV, B, D>, queue::Change> {
        let released = match self.flight.complete(target, bytes) {
            Completion::Stale => return Err(queue::Change::Stale),
            Completion::Invalid(error) => return Err(queue::Change::Invalid(error)),
            Completion::Ready(released) => released,
        };
        Ok(queue::Released::new(self.queue.entries, released))
    }
}

const _: () = assert!(
    mem::size_of::<Output<'static, 'static, 'static, 32, ()>>() == 4 * mem::size_of::<usize>()
);

/// One retained nonempty source prefix before descriptor reservation.
pub(in crate::link) struct Plan<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode> {
    active: &'flight mut flight::Flight<'queue, 'p, 'd, IOV>,
    source: metadata::Prefix<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
}

/// Prepared retains this queue's immutable suffixes and exact descriptor sum.
/// Released checks that sum once; Output and Prepared preserve the original queue.
/// Each whole front transfers its exact successor; a partial front stays linked.
pub(in crate::link::egress) struct Released<'a, 'p, 'd, const IOV: usize> {
    active: flight::Active<'a, 'p, 'd, IOV>,
    bytes: transfer::Len,
}

impl<'a, 'p, 'd, const IOV: usize> Released<'a, 'p, 'd, IOV> {
    pub(in crate::link::egress::queue) fn consume<'queue, B, D: demand::Mode>(
        self,
        entries: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
        token: &mut region::Token<region::Identity<'d>>,
        mut release: impl FnMut(front::Consumed<B>) -> credit::Prefix,
    ) {
        let Some(mut bytes) = self.bytes.nonzero() else {
            return;
        };
        let mut prefix = self.active.lease.into_prefix();
        loop {
            let (front, next) = front::advance(entries, prefix, token, bytes);
            match release(front) {
                credit::Prefix::Whole { remaining: rest } => {
                    let Some(next) = next else {
                        return;
                    };
                    let Some(left) = rest.nonzero() else {
                        return;
                    };
                    prefix = next;
                    bytes = left;
                }
                credit::Prefix::Partial => return,
            }
        }
    }
}

impl<'flight, 'queue, 'p, 'd, const IOV: usize, B: data::raw::Payload<'d>, D: demand::Mode>
    Plan<'flight, 'queue, 'p, 'd, IOV, B, D>
{
    fn new(output: &'flight mut flight::Output<'queue, 'p, 'd, IOV, B, D>) -> Option<Self> {
        const { assert!(IOV != 0 && IOV <= msg::MAX_IOVECS) };
        let source = output.queue.entries.prefix()?;
        Some(Self {
            active: &mut output.flight,
            source,
        })
    }

    pub(in crate::link) fn reserve(
        self,
        token: &region::Token<region::Identity<'d>>,
    ) -> prepared::Reservation<'flight, 'queue, 'p, 'd, IOV, B, D> {
        if self.active.lease.is_some() {
            return prepared::Reservation::Active;
        }
        let Some(buffer) = self.active.flights.take() else {
            return prepared::Reservation::Unavailable;
        };
        let mut buffer = Buffer(buffer);
        // SAFETY: this vacant pinned entry is exclusively held before submission.
        let flight = unsafe { buffer.0.get_mut().get_unchecked_mut() };
        let mut bytes = progress::State::new(transfer::Len::MAX);
        let (entries, prefix) = self.source.visit(token, |view| {
            let mut cursor = view.chunks();
            while let Some(vacant) = flight.iovecs.vacancy() {
                let Some(chunk) = bytes.scatter(&mut cursor) else {
                    return if bytes.is_empty() {
                        ops::ControlFlow::Break(())
                    } else {
                        ops::ControlFlow::Continue(())
                    };
                };
                // SAFETY: this reservation retains the visited queue and its exact prefix.
                let iovec = unsafe { msg::raw::Iovec::retain(chunk.bytes()) };
                vacant.insert(iovec);
            }
            ops::ControlFlow::Break(())
        });
        flight.bytes = bytes;
        let reserved = flight::Prepared {
            lease: self.active.lease,
            reservation: Lease { buffer, prefix },
        };
        prepared::Reservation::Ready(prepared::Prepared::new(reserved, entries))
    }
}
