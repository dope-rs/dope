mod entry;
use std::{marker, mem, ops};

use dope_core::io::{socket::msg, transfer, transfer::progress};
use entry::Buffer;
pub(in crate::link::egress) use entry::{Entry, Lease};
use o3::{base::cell::region, buffer::resident, depot::arena::pool::recycle};

use crate::{
    link::egress::{
        self, data,
        load::credit,
        metadata,
        metadata::{arena, front, lane},
        queue,
        reserve::demand,
    },
    wire::send,
};

pub(in crate::link) enum Preparation<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode>
{
    Empty,
    Ready(Plan<'flight, 'queue, 'p, 'd, IOV, B, D>),
}

pub(in crate::link) enum Reservation<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode>
{
    Ready(Prepared<'flight, 'queue, 'p, 'd, IOV, B, D>),
    Active,
    Unavailable,
}

#[must_use]
pub(in crate::link) struct Prepared<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode> {
    lease: &'flight mut Option<Lease<'p, 'd, IOV>>,
    reservation: Lease<'p, 'd, IOV>,
    entries: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
}

struct Active<'a, 'p, 'd, const IOV: usize> {
    slot: &'a mut Option<Lease<'p, 'd, IOV>>,
    lease: Lease<'p, 'd, IOV>,
}

impl<'flight, 'queue, 'p, 'd, const IOV: usize, B: data::raw::Payload<'d>, D: demand::Mode>
    Prepared<'flight, 'queue, 'p, 'd, IOV, B, D>
{
    pub(in crate::link) fn retain(self) {
        *self.lease = Some(self.reservation);
    }

    pub(in crate::link) fn vectored(&mut self) -> send::Vectored<'_, IOV> {
        self.reservation.get_mut().vectored()
    }

    pub(in crate::link) fn release(
        self,
        bytes: transfer::Len,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), egress::Invalid> {
        let active = Active {
            slot: self.lease,
            lease: self.reservation,
        };
        let released = match active.release(bytes) {
            Ok(released) => released,
            Err((_, error)) => return Err(error),
        };
        released.consume(self.entries, token, &mut drop);
        Ok(())
    }
}

impl<'a, 'p, 'd, const IOV: usize> Active<'a, 'p, 'd, IOV> {
    fn release(
        self,
        bytes: transfer::Len,
    ) -> Result<Released<'a, 'p, 'd, IOV>, (Self, egress::Invalid)> {
        let maximum = self.lease.get().len();
        if bytes > maximum {
            return Err((
                self,
                egress::Invalid::Excess {
                    actual: bytes,
                    maximum,
                },
            ));
        }
        Ok(Released {
            prefix: self.lease.into_prefix(),
            bytes,
            loan: marker::PhantomData,
        })
    }

    fn restore(self) {
        *self.slot = Some(self.lease);
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Lease<'static, 'static, 32>>() == 3 * word);
    assert!(mem::size_of::<Option<Lease<'static, 'static, 32>>>() == 3 * word);
    assert!(mem::size_of::<Released<'static, 'static, 'static, 32>>() == 3 * word);
    assert!(mem::size_of::<Entry<32>>() <= 768);
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
            Preparation<
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
    lease: &'a mut Option<Lease<'p, 'd, IOV>>,
    flights: &'p recycle::Store<Entry<IOV>>,
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
        flights: &'p recycle::Store<Entry<IOV>>,
    ) -> Self {
        let (lane, entries) = slot.into_parts();
        Self {
            queue: egress::Queue { entries },
            lease: lane,
            flights,
        }
    }

    pub(in crate::link) fn is_active(&self) -> bool {
        self.lease.is_some()
    }
    pub(in crate::link) fn abort(&mut self) {
        drop(self.lease.take());
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
    ) -> Preparation<'flight, 'a, 'p, 'd, IOV, B, D> {
        let Some(plan) = Plan::new(self) else {
            return Preparation::Empty;
        };
        Preparation::Ready(plan)
    }

    pub(in crate::link) fn complete(
        &mut self,
        token: &mut region::Token<region::Identity<'d>>,
        bytes: transfer::Len,
        mut release: impl FnMut(B),
    ) -> queue::Change {
        let Some(lease) = self.lease.take() else {
            return queue::Change::Stale;
        };
        let active = Active {
            slot: self.lease,
            lease,
        };
        let released = match active.release(bytes) {
            Ok(released) => released,
            Err((active, error)) => {
                active.restore();
                return queue::Change::Invalid(error);
            }
        };
        released.consume(self.queue.entries, token, &mut release);
        queue::Change::Applied
    }
}

const _: () = assert!(
    mem::size_of::<Output<'static, 'static, 'static, 32, ()>>() == 4 * mem::size_of::<usize>()
);

/// One retained nonempty source prefix before descriptor reservation.
pub(in crate::link) struct Plan<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode> {
    output: &'flight mut Output<'queue, 'p, 'd, IOV, B, D>,
    source: metadata::Prefix<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
}

/// Prepared retains this queue's immutable suffixes and exact descriptor sum.
/// Released checks that sum once; Output and Prepared preserve the original queue.
/// Each whole front transfers its exact successor; a partial front stays linked.
struct Released<'a, 'p, 'd, const IOV: usize> {
    prefix: lane::Prefix<'p, 'd>,
    bytes: transfer::Len,
    loan: marker::PhantomData<&'a mut Option<Lease<'p, 'd, IOV>>>,
}

impl<'a, 'p, 'd, const IOV: usize> Released<'a, 'p, 'd, IOV> {
    fn consume<'queue, B, D: demand::Mode>(
        self,
        entries: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
        token: &mut region::Token<region::Identity<'d>>,
        release: &mut impl FnMut(B),
    ) {
        let Some(mut bytes) = self.bytes.nonzero() else {
            return;
        };
        let mut prefix = self.prefix;
        loop {
            let (front, next) = front::advance(entries, prefix, token, bytes);
            match front.complete(release) {
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
    fn new(output: &'flight mut Output<'queue, 'p, 'd, IOV, B, D>) -> Option<Self> {
        const { assert!(IOV != 0 && IOV <= msg::MAX_IOVECS) };
        let source = output.queue.entries.prefix()?;
        Some(Self { output, source })
    }

    pub(in crate::link) fn reserve(
        self,
        token: &region::Token<region::Identity<'d>>,
    ) -> Reservation<'flight, 'queue, 'p, 'd, IOV, B, D> {
        if self.output.lease.is_some() {
            return Reservation::Active;
        }
        let Some(buffer) = self.output.flights.take() else {
            return Reservation::Unavailable;
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
        flight.bytes = bytes.len();
        Reservation::Ready(Prepared {
            lease: self.output.lease,
            reservation: Lease { buffer, prefix },
            entries,
        })
    }
}
