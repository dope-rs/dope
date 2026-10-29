use crate::{driver::life::finish, io::fd::handles};
pub mod cancel;
mod complete;
pub(crate) mod raw;
mod request;
mod slots;
pub mod storage;
use std::{marker, mem};

pub(crate) use complete::Complete;
use dope_sys::{engine::admit::Cancelled, table::token::host};
use o3::{alloc, base::marker::ThreadBound};
pub(crate) use request::Request;
use route::table;
pub use slots::Slots;
use table::conn;

use crate::{
    driver::{
        self,
        route::{self, kind},
    },
    io::fd::domain::retire,
};

type Invariant<'d> = (driver::Invariant<'d>, marker::PhantomData<ThreadBound>);

const INDEX_BITS: u32 = route::SLOT_BITS;
const GENERATION_BITS: u32 = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct Target {
    epoch: route::Epoch,
    slot: [u8; 3],
    auxiliary: [u8; 3],
    kind: kind::Native,
    route: route::Route,
}

impl Target {
    fn new<Tag: route::Tag, C: kind::Flight>(target: route::Operation<'_, Tag, C>) -> Self {
        let parts = target.parts();
        Self {
            epoch: parts.epoch(),
            slot: parts.slot().into_le24(),
            auxiliary: [0; 3],
            kind: C::FLIGHT,
            route: Tag::ROUTE,
        }
    }

    fn token(self) -> route::Token {
        let [a, b, c] = self.slot;
        let [d, e, f] = self.auxiliary;
        route::Token::from_flight(self.epoch, [a, b, c, d, e, f], self.kind, self.route)
    }

    fn bind<'d, C: kind::Class>(self, driver: driver::Invariant<'d>) -> route::Erased<'d, C> {
        route::Erased {
            token: self.token(),
            driver,
            class: marker::PhantomData,
        }
    }
}

#[repr(transparent)]
pub(crate) struct Arena(storage::Flights<Target, INDEX_BITS, GENERATION_BITS>);

/// Exact kernel-flight capacity derived from an existing typed resource bound.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Capacity(usize);

/// Typed flight-group requirements retained until their shared preparation.
pub struct Need<Tags, const GROUPS: usize = 1> {
    caps: [Capacity; GROUPS],
    tags: marker::PhantomData<Tags>,
}

/// Exact connection scaling rejected by the kernel-flight slot bound.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum ScaleError {
    Product {
        capacity: conn::Capacity,
        factor: usize,
    },
    Limit {
        actual: usize,
        maximum: usize,
    },
}

impl Capacity {
    pub const fn fixed<const FLIGHTS: usize>() -> Self {
        const { assert!(FLIGHTS > 0 && FLIGHTS <= route::SLOT_MASK as usize + 1) };
        Self(FLIGHTS)
    }

    pub const fn table(capacity: table::Capacity) -> Self {
        Self(capacity.count().get())
    }

    pub const fn connections<const PER_CONNECTION: usize>(
        capacity: conn::Capacity,
    ) -> Result<Self, ScaleError> {
        const { assert!(PER_CONNECTION > 0) };
        let Some(total) = (capacity.raw() as usize).checked_mul(PER_CONNECTION) else {
            return Err(ScaleError::Product {
                capacity,
                factor: PER_CONNECTION,
            });
        };
        let maximum = route::SLOT_MASK as usize + 1;
        if total > maximum {
            return Err(ScaleError::Limit {
                actual: total,
                maximum,
            });
        }
        Ok(Self(total))
    }

    pub const fn capped<const MAX: usize>(capacity: table::Capacity) -> Self {
        const { assert!(MAX > 0) };
        let capacity = capacity.count().get();
        Self(if capacity < MAX { capacity } else { MAX })
    }

    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

impl<Tag: route::Tag> Need<Tag> {
    pub const fn new(cap: Capacity) -> Self {
        Self {
            caps: [cap],
            tags: marker::PhantomData,
        }
    }
}

impl<A: route::Tag> Need<A> {
    pub fn pair<B: route::Tag>(self, other: Need<B>) -> Need<(A, B), 2> {
        let Self {
            caps: [first],
            tags: marker::PhantomData,
        } = self;
        let Need {
            caps: [second],
            tags: marker::PhantomData,
        } = other;
        Need {
            caps: [first, second],
            tags: marker::PhantomData,
        }
    }
}

impl<A: route::Tag, B: route::Tag> Need<(A, B), 2> {
    pub fn push<C: route::Tag>(self, other: Need<C>) -> Need<(A, B, C), 3> {
        let Self {
            caps: [first, second],
            tags: marker::PhantomData,
        } = self;
        let Need {
            caps: [third],
            tags: marker::PhantomData,
        } = other;
        Need {
            caps: [first, second, third],
            tags: marker::PhantomData,
        }
    }
}

impl<Tags, const GROUPS: usize> Need<Tags, GROUPS> {
    const fn into_caps(self) -> [Capacity; GROUPS] {
        self.caps
    }
}

#[must_use = "prepared flight slots must be committed"]
pub struct Prepared<'a, 'd, Tags, const GROUPS: usize = 1> {
    prepared: storage::Prepared<'a, 'd, Target, INDEX_BITS, GENERATION_BITS, GROUPS>,
    driver: driver::Invariant<'d>,
    tags: marker::PhantomData<Tags>,
}

#[must_use = "a validated flight vacancy must be reserved or released"]
pub(crate) struct Vacant<'slot, 'd, Tag: route::Tag, C: kind::Class = <Tag as route::Tag>::Class> {
    vacancy: storage::Vacant<'slot, 'd, Target, INDEX_BITS, GENERATION_BITS>,
    target: Target,
    driver: Invariant<'d>,
    class: marker::PhantomData<(Tag, C)>,
}

#[must_use = "a reserved flight must be committed or released"]
#[repr(transparent)]
pub(crate) struct Reserved<'d, C: kind::Class = kind::Any> {
    reservation: storage::Reserved<'d, Target, INDEX_BITS, GENERATION_BITS>,
    driver: Invariant<'d>,
    class: marker::PhantomData<C>,
}

/// Exact kernel-flight authority bound invariantly to its driver.
/// Dropping this owner preserves any pending native completion.
#[must_use = "a live flight must reach terminal completion or quiescence"]
#[repr(transparent)]
pub struct Flight<'d, C: kind::Class = kind::Any> {
    flight: storage::Lease<'d, Target, INDEX_BITS, GENERATION_BITS>,
    driver: Invariant<'d>,
    class: marker::PhantomData<C>,
}

pub(crate) struct Drain<'q, 'd> {
    drain: storage::Drain<'q, Target, INDEX_BITS, GENERATION_BITS>,
    driver: driver::Reference<'d>,
}

impl Arena {
    pub(crate) fn new() -> Self {
        Self(storage::Flights::new())
    }

    pub(in crate::driver) fn is_pending(&self) -> bool {
        self.0.is_pending()
    }

    pub(in crate::driver) fn finish(&self) {
        self.0.finish();
    }

    pub(crate) fn try_prepare<'a, 'd, Tags, const GROUPS: usize>(
        &'a mut self,
        need: Need<Tags, GROUPS>,
        driver: driver::Reference<'d>,
    ) -> Result<Prepared<'a, 'd, Tags, GROUPS>, alloc::Error> {
        let access = raw::Access::new(&mut self.0, driver);
        let (prepared, driver) = access.try_batch(need.into_caps().map(Capacity::get))?;
        Ok(Prepared {
            prepared,
            driver,
            tags: marker::PhantomData,
        })
    }
}

impl<'d, Tags, const GROUPS: usize> Prepared<'_, 'd, Tags, GROUPS> {
    fn commit(
        self,
    ) -> (
        [storage::Slots<'d, Target, INDEX_BITS, GENERATION_BITS>; GROUPS],
        Invariant<'d>,
    ) {
        let Self {
            prepared,
            driver,
            tags: marker::PhantomData,
        } = self;
        let slots = prepared.groups();
        (slots, (driver, marker::PhantomData))
    }
}

impl<'d, Tag: route::Tag> Prepared<'_, 'd, Tag> {
    pub fn slots(self) -> Slots<'d, Tag> {
        let ([slots], driver) = self.commit();
        Slots::new(slots, driver)
    }
}

impl<'d, A: route::Tag, B: route::Tag> Prepared<'_, 'd, (A, B), 2> {
    pub fn slots(self) -> (Slots<'d, A>, Slots<'d, B>) {
        let ([first, second], driver) = self.commit();
        (Slots::new(first, driver), Slots::new(second, driver))
    }
}

impl<'d, A: route::Tag, B: route::Tag, C: route::Tag> Prepared<'_, 'd, (A, B, C), 3> {
    pub fn slots(self) -> (Slots<'d, A>, Slots<'d, B>, Slots<'d, C>) {
        let ([first, second, third], driver) = self.commit();
        (
            Slots::new(first, driver),
            Slots::new(second, driver),
            Slots::new(third, driver),
        )
    }
}

impl<'d, Tag: route::Tag, C: kind::Class> Vacant<'_, 'd, Tag, C> {
    pub(crate) fn reserve(self) -> Reserved<'d, C> {
        Reserved {
            reservation: self.vacancy.reserve(self.target),
            driver: self.driver,
            class: marker::PhantomData,
        }
    }
}

impl<'d, C: kind::Class> Reserved<'d, C> {
    pub(crate) fn key(&self) -> raw::Echo {
        raw::Echo::from_inner(self.reservation.echo())
    }

    pub(crate) fn commit(self) -> Flight<'d, C> {
        Flight {
            flight: self.reservation.commit(),
            driver: self.driver,
            class: marker::PhantomData,
        }
    }
}

impl<'q, 'd> Drain<'q, 'd> {
    pub(crate) fn new(arena: &'q Arena, driver: driver::Reference<'d>) -> Self {
        Self {
            drain: arena.0.drain(),
            driver,
        }
    }
}

impl<'q, 'd> host::Drain for Drain<'q, 'd> {
    type Driver = driver::Reference<'d>;

    fn driver(&self) -> Self::Driver {
        self.driver
    }

    type Key = raw::Echo;
    type Returned = retire::Returned;
    type Flight = Complete<'q, 'd>;
    type Echo = retire::Echo<'d>;
    type Class = handles::Retirement;

    fn bind(&self, key: raw::Echo) -> Option<Complete<'q, 'd>> {
        self.drain.bind(key.into_inner()).map(|bound| Complete {
            completion: bound,
            driver: self.driver,
            class: marker::PhantomData,
        })
    }

    fn retired(&self, returned: Self::Returned) -> host::Closed<Self::Echo, Self::Class> {
        use host::Closed;
        match self.driver.descriptors().retirement().bind(returned) {
            retire::Bind::Ready(echo) => Closed::Ready(echo),
            retire::Bind::Class {
                echo,
                expected,
                observed,
            } => Closed::Class {
                echo,
                expected,
                observed,
            },
            retire::Bind::Missing => Closed::Missing,
        }
    }
}

impl<'d, C: kind::Class> Flight<'d, C> {
    pub(crate) fn is_pending(&self) -> bool {
        self.flight.is_pending()
    }

    pub(crate) fn key(&self) -> raw::Echo {
        raw::Echo::from_inner(self.flight.echo())
    }

    pub fn target(&self) -> route::Erased<'d, C> {
        self.flight.value().bind(self.driver.0)
    }

    pub fn matches(&self, target: route::Erased<'d, C>) -> bool {
        self.target().matches(target)
    }

    pub(in crate::driver) fn finish(mut self, proof: &mut finish::Finish<'_, 'd>) {
        self.driver.0 = proof.brand();
    }
}

const _: () = {
    assert!(mem::size_of::<Arena>() == 4 * mem::size_of::<usize>());
    assert!(mem::align_of::<Arena>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Target>() == mem::size_of::<route::Token>());
    assert!(mem::align_of::<Target>() == mem::align_of::<route::Token>());
    assert!(mem::size_of::<raw::Echo>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Flight<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Flight<'static>>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Reserved<'static>>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<Vacant<'static, 'static, route::KeyTag<1>>>()
            == mem::size_of::<storage::Vacant<'static, 'static, Target, INDEX_BITS, GENERATION_BITS>>(
            ) + mem::size_of::<route::Token>()
    );
    assert!(mem::size_of::<Drain<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Complete<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Slots<'static, route::KeyTag<1>>>() == mem::size_of::<usize>());
};

pub(crate) struct Mismatch<'q, 'd> {
    pub(crate) bound: Complete<'q, 'd>,
    pub(crate) expected: kind::Native,
    pub(crate) observed: kind::Native,
}

impl<'d, C: kind::Class> Flight<'d, C> {
    /// Submits cancellation for this exact flight without releasing its owners.
    pub fn try_cancel(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Cancelled, cancel::Error> {
        driver.native().try_cancel(self)
    }
}

impl<'d, C: kind::Class> Flight<'d, C> {
    /// Cancellation reserves no new flight, so capacity refusal is queue fullness.
    pub fn try_stop(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<cancel::Outcome, cancel::Failure> {
        let status = match self.try_cancel(driver) {
            Ok(status) => status,
            Err(cancel::Error::Full(_)) => {
                return Ok(cancel::Outcome::Deferred);
            }
            Err(cancel::Error::Rejected(cause)) => return Err(cause),
        };
        Ok(match status {
            Cancelled::Submitted => cancel::Outcome::Submitted,
            Cancelled::Queued => cancel::Outcome::Queued,
            Cancelled::Completed => cancel::Outcome::Completed,
        })
    }
}
