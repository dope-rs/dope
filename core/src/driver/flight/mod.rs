pub(crate) mod raw;

use std::{fmt, io, marker, mem, rc};

use o3::collections::completion::narrow;

use crate::driver::{self, route};

type Invariant<'d> = marker::PhantomData<(fn(&'d ()) -> &'d (), rc::Rc<()>)>;

const INDEX_BITS: u32 = route::SLOT_BITS;
const GENERATION_BITS: u32 = 32;
type InnerArena = narrow::Arena<route::Token, INDEX_BITS, GENERATION_BITS>;
type InnerEcho = narrow::Echo<route::Token, INDEX_BITS, GENERATION_BITS>;
type InnerSlots<'d> = narrow::Slots<'d, route::Token, INDEX_BITS, GENERATION_BITS>;
type InnerReservation<'d> = narrow::Reservation<'d, route::Token, INDEX_BITS, GENERATION_BITS>;
type InnerLease<'d> = narrow::Lease<'d, route::Token, INDEX_BITS, GENERATION_BITS>;
type InnerResolved<'q> = narrow::Resolved<'q, route::Token, INDEX_BITS, GENERATION_BITS>;
type InnerDrain<'q> = narrow::Drain<'q, route::Token, INDEX_BITS, GENERATION_BITS>;

#[repr(transparent)]
pub(crate) struct Arena(InnerArena);

struct Owner<'a, 'd> {
    arena: &'a mut InnerArena,
    driver: driver::Reference<'d>,
}

#[doc(hidden)]
#[repr(transparent)]
pub struct Slots<'d, Tag: route::Tag> {
    slots: InnerSlots<'d>,
    driver: Invariant<'d>,
    tag: marker::PhantomData<Tag>,
}

#[must_use = "a reserved flight must be committed or released"]
#[repr(transparent)]
pub(crate) struct Reservation<'d> {
    reservation: InnerReservation<'d>,
    driver: Invariant<'d>,
}

#[must_use = "a live flight must reach terminal completion or quiescence"]
#[repr(transparent)]
pub struct Flight<'d> {
    flight: InnerLease<'d>,
    driver: Invariant<'d>,
}

#[repr(transparent)]
pub(crate) struct Completion<'q, 'd> {
    completion: InnerResolved<'q>,
    driver: Invariant<'d>,
}

pub(crate) struct Drain<'q, 'd> {
    drain: InnerDrain<'q>,
    driver: driver::Reference<'d>,
}

const _: () = {
    assert!(mem::size_of::<raw::Echo>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Flight<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Flight<'static>>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Reservation<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Completion<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Drain<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Slots<'static, route::KeyTag<1>>>() == mem::size_of::<usize>());
};

impl Arena {
    pub(crate) const fn new() -> Self {
        Self(InnerArena::new())
    }

    pub(crate) fn try_slots<'d, Tag: route::Tag>(
        &mut self,
        capacity: usize,
        driver: driver::Reference<'d>,
    ) -> io::Result<Slots<'d, Tag>> {
        Ok(Slots {
            slots: InnerArena::try_slots(
                Owner {
                    arena: &mut self.0,
                    driver,
                },
                capacity,
            )?,
            driver: marker::PhantomData,
            tag: marker::PhantomData,
        })
    }
}

impl<'d, Tag: route::Tag> Slots<'d, Tag> {
    pub(crate) fn reserve(&self, target: route::Operation<'d, Tag>) -> Option<Reservation<'d>> {
        if Tag::KIND != 0 && target.kind() != Tag::KIND {
            return None;
        }
        Some(Reservation {
            reservation: self.slots.reserve(target.into_token())?,
            driver: marker::PhantomData,
        })
    }
}

impl<'d> Reservation<'d> {
    pub(crate) fn key(&self) -> raw::Echo {
        raw::Echo::from_inner(self.reservation.key().echo())
    }

    pub(crate) fn commit(self) -> Flight<'d> {
        Flight {
            flight: self.reservation.commit(),
            driver: marker::PhantomData,
        }
    }
}

impl<'q, 'd> Drain<'q, 'd> {
    pub(in crate::driver) fn new(arena: &'q Arena, driver: driver::Reference<'d>) -> Self {
        Self {
            drain: arena.0.drain(),
            driver,
        }
    }

    pub(crate) fn driver(&self) -> driver::Reference<'d> {
        self.driver
    }

    pub(crate) fn complete(&self, key: raw::Echo) -> Option<Completion<'q, 'd>> {
        Some(Completion {
            completion: self.drain.complete(key.into_inner())?,
            driver: marker::PhantomData,
        })
    }
}

impl Completion<'_, '_> {
    pub(crate) fn resolve(self, more: bool) -> Option<route::Token> {
        self.completion.resolve(more)
    }
}

impl<'d> Flight<'d> {
    pub(crate) fn key(&self) -> raw::Echo {
        raw::Echo::from_inner(self.flight.key().echo())
    }

    pub fn target(&self) -> route::Token {
        self.flight.value()
    }

    pub(crate) fn target_erased(&self) -> route::Erased<'d> {
        route::Erased::new(self.target())
    }

    pub fn matches(&self, target: route::Token) -> bool {
        self.target() == target
    }

    pub fn complete(self) -> route::Token {
        self.flight.complete()
    }
}

impl fmt::Debug for Flight<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Flight")
            .field("target", &self.target())
            .finish_non_exhaustive()
    }
}
