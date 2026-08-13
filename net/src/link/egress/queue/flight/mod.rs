use std::{marker, mem, pin, ptr};

use dope_core::{
    driver::route,
    io::{socket::msg, transfer},
};
use o3::{
    cell::region,
    collections::{self, fixed::pinned::recycle, slab},
};

use crate::wire::send;

mod sealed;

pub(super) use sealed::Access;

/// Pinned descriptor storage for one in-flight vectored send.
struct Flight<const IOV: usize> {
    iovecs: [msg::Iovec; IOV],
    header: msg::Header,
    len: usize,
    bytes: transfer::Len,
    target: Option<route::Token>,
    _pin: marker::PhantomPinned,
}

impl<const IOV: usize> Flight<IOV> {
    pub(in crate::link::egress::queue) fn new() -> Self {
        Self {
            iovecs: [msg::Iovec::empty(); IOV],
            header: msg::Header::new(),
            len: 0,
            bytes: transfer::Len::ZERO,
            target: None,
            _pin: marker::PhantomPinned,
        }
    }

    fn is_empty(self: pin::Pin<&Self>) -> bool {
        self.bytes == transfer::Len::ZERO
    }

    fn len(self: pin::Pin<&Self>) -> usize {
        self.len
    }

    fn bytes(self: pin::Pin<&Self>) -> usize {
        self.bytes.into_usize()
    }

    fn matches(self: pin::Pin<&Self>, target: route::Token) -> bool {
        self.target
            .is_some_and(|current| current.same_target(target))
    }
}

impl<const IOV: usize> recycle::Recycle for Flight<IOV> {
    fn recycle(self: pin::Pin<&mut Self>) {
        self.reset();
    }
}

#[repr(transparent)]
pub(in crate::link::egress) struct Lease<'d, const IOV: usize>(recycle::Lease<'d, Flight<IOV>>);

const _: () = assert!(mem::size_of::<Option<Lease<'static, 32>>>() == mem::size_of::<usize>());

const _: () = assert!(mem::size_of::<Flight<32>>() <= 768);

pub(in crate::link::egress) struct Pool<'d, const IOV: usize> {
    pool: recycle::Pool<Flight<IOV>>,
    _domain: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

struct Source<'d, const IOV: usize> {
    pool: ptr::NonNull<recycle::Pool<Flight<IOV>>>,
    _domain: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, const IOV: usize> Pool<'d, IOV> {
    pub(in crate::link::egress) fn try_with_capacity(
        _domain: &region::Token<'d>,
        capacity: u32,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            pool: recycle::Pool::try_with_capacity(slab::Capacity::new(capacity), |_| {
                Flight::new()
            })?,
            _domain: marker::PhantomData,
        })
    }

    fn reserve(&self) -> Option<recycle::Reservation<'d, Flight<IOV>>> {
        recycle::Pool::reserve_owned(Source {
            pool: ptr::NonNull::from(&self.pool),
            _domain: marker::PhantomData,
        })
    }
}

pub(in crate::link::egress) struct State<'a, 'd, const IOV: usize> {
    lease: &'a mut Option<Lease<'d, IOV>>,
    flights: &'a mut Pool<'d, IOV>,
}

impl<'a, 'd, const IOV: usize> State<'a, 'd, IOV> {
    pub(in crate::link::egress::queue) fn new(
        lease: &'a mut Option<Lease<'d, IOV>>,
        flights: &'a mut Pool<'d, IOV>,
    ) -> Self {
        Self { lease, flights }
    }

    pub(in crate::link::egress::queue) fn reborrow(&mut self) -> State<'_, 'd, IOV> {
        State::new(self.lease, self.flights)
    }

    pub(in crate::link::egress::queue) fn begin(&mut self) -> Option<Prepared<'_, 'd, IOV>> {
        if self.lease.is_some() {
            return None;
        }
        Some(Prepared {
            lease: self.lease,
            reservation: self.flights.reserve()?,
        })
    }

    fn active(&self) -> Option<Active<'_, 'd, IOV>> {
        let Lease(lease) = self.lease.as_ref()?;
        Some(Active { lease })
    }

    fn release(&mut self) {
        drop(self.lease.take());
    }

    pub(in crate::link::egress::queue) fn is_active(&self) -> bool {
        self.lease.is_some()
    }

    pub(in crate::link::egress::queue) fn complete(
        &mut self,
        target: route::Token,
        bytes: usize,
    ) -> Option<Released<IOV>> {
        let active = self.active()?;
        if !active.matches(target) {
            return None;
        }
        let released = active.release(bytes)?;
        self.release();
        Some(released)
    }

    pub(in crate::link::egress::queue) fn abort(&mut self, target: route::Token) -> bool {
        let Some(active) = self.active() else {
            return false;
        };
        if !active.matches(target) {
            return false;
        }
        self.release();
        true
    }
}

#[must_use]
pub(in crate::link) struct Prepared<'a, 'd, const IOV: usize> {
    lease: &'a mut Option<Lease<'d, IOV>>,
    reservation: recycle::Reservation<'d, Flight<IOV>>,
}

struct Active<'a, 'd, const IOV: usize> {
    lease: &'a recycle::Lease<'d, Flight<IOV>>,
}

/// Proof that an acknowledged prefix came from one descriptor-bounded flight.
pub(in crate::link) struct Released<const IOV: usize> {
    bytes: usize,
    entries: usize,
}

impl<const IOV: usize> Released<IOV> {
    pub(in crate::link::egress::queue) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::link::egress::queue) fn take_entry(&mut self) -> bool {
        if self.entries == 0 {
            return false;
        }
        self.entries -= 1;
        true
    }
}

impl<const IOV: usize> Prepared<'_, '_, IOV> {
    fn flight(&self) -> pin::Pin<&Flight<IOV>> {
        self.reservation.get()
    }

    fn flight_mut(&mut self) -> pin::Pin<&mut Flight<IOV>> {
        self.reservation.get_mut()
    }

    pub(in crate::link) fn is_empty(&self) -> bool {
        self.flight().is_empty()
    }

    pub(in crate::link) fn len(&self) -> usize {
        self.flight().len()
    }

    pub(in crate::link) fn bytes(&self) -> usize {
        self.flight().bytes()
    }

    pub(in crate::link) fn push(&mut self, iov: msg::Iovec) -> bool {
        self.flight_mut().push(iov)
    }

    pub(in crate::link) fn retain(mut self, target: route::Token) {
        self.flight_mut().mark(target);
        *self.lease = Some(Lease(self.reservation.commit()));
    }

    pub(in crate::link) fn vectored(&mut self) -> send::Vectored<'_> {
        self.flight_mut().vectored()
    }

    pub(in crate::link) fn release(self, bytes: usize) -> Option<Released<IOV>> {
        let released = (bytes <= self.flight().bytes()).then(|| Released {
            bytes,
            entries: self.len(),
        })?;
        Some(released)
    }
}

impl<const IOV: usize> Active<'_, '_, IOV> {
    fn flight(&self) -> pin::Pin<&Flight<IOV>> {
        self.lease.get()
    }

    fn matches(&self, target: route::Token) -> bool {
        self.flight().matches(target)
    }

    fn release(&self, bytes: usize) -> Option<Released<IOV>> {
        (bytes <= self.flight().bytes()).then(|| Released {
            bytes,
            entries: self.flight().len(),
        })
    }
}
