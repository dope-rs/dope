use std::marker;

use dope_core::driver::{
    receive::credit,
    route::{Identity, KeyTag, table::conn},
};
use o3::depot::queue::tagged::entry;
use receive::mode;

use crate::wire::{self, receive, terminal};

mod borrowed;
mod direct;

pub use borrowed::Borrowed;
pub use direct::Direct;

/// A resource wait which owns the runtime borrow needed to register it.
pub trait Wait<'d, ID: Identity, W: wire::Spec> {
    type Registration: Registration;

    fn register(
        self,
        credit: credit::Pending<'d, ID>,
    ) -> Result<Self::Registration, entry::Error<conn::Capacity>>;
}

/// A provisional resource wait committed after its completion is stored.
pub trait Registration {
    fn commit(self);
}

/// A completed receive reservation transaction.
pub trait Completion {
    fn complete(self);
}

/// Infallible reservation kinds selected before receive dispatch.
pub(crate) trait Immediate<W: wire::Spec>: Kind<W, Wait = mode::Disabled> {
    fn reserve<'a, 'w, 'd, ID: Identity + 'w>(
        wire: &'a mut W::Connection<'w, ID>,
        run: &'a mut W::Runtime<'w, 'd, ID>,
    ) -> Self::Transaction<'a, 'w, 'd, ID>
    where
        W: 'w,
        'd: 'w,
        'w: 'a;
}

/// Static reservation shape selected by a receive policy.
pub trait Kind<W: wire::Spec> {
    type Wait: mode::Flag;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>
    where
        W: 'w,
        'd: 'w,
        'w: 'a;
}

/// Exact outcome of reserving receive resources.
#[must_use]
pub enum Outcome<T, B, E> {
    /// Owns every resource needed to process the observed receive.
    Ready(T),
    /// Owns the borrow needed to register a recoverable resource wait.
    Blocked(B),
    /// Owns the exact terminal cause; no receive waiter will be registered.
    Failed(E),
}

/// Reservation selected only by policies with real resource waiters.
pub trait Try<W: wire::Spec> {
    type Block<'a, 'w, 'd, ID: Identity + 'w>: Wait<'d, ID, W>
    where
        W: 'w,
        'd: 'w,
        'w: 'a;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>: Completion
    where
        W: 'w,
        'd: 'w,
        'w: 'a;

    /// Returns all provisional resources before reporting failure, leaving
    /// the wire ready for terminal drain with its exact owned cause.
    fn try_reserve<'a, 'w, 'd, ID: Identity + 'w>(
        wire: &'a mut W::Connection<'w, ID>,
        send: &'a mut W::SendStorage<'w>,
        run: &'a mut W::Runtime<'w, 'd, ID>,
    ) -> Outcome<
        Self::Transaction<'a, 'w, 'd, ID>,
        Self::Block<'a, 'w, 'd, ID>,
        terminal::Error<'w, W>,
    >
    where
        W: 'w,
        'd: 'w,
        'w: 'a;

    fn cancel<'w, 'd, ID: Identity + 'w>(
        run: &mut W::Runtime<'w, 'd, ID>,
        target: credit::Id<'d, ID>,
    ) where
        W: 'w,
        'd: 'w;

    fn release_recv<'w, 'd, ID: Identity + 'w>(run: &mut W::Runtime<'w, 'd, ID>)
    where
        W: 'w,
        'd: 'w;

    fn release_send<'w, 'd, ID: Identity + 'w>(run: &mut W::Runtime<'w, 'd, ID>)
    where
        W: 'w,
        'd: 'w;
}

/// Static selection of a recoverable reservation policy.
pub struct Waiting<P>(marker::PhantomData<fn() -> P>);

impl<W, P> Kind<W> for Waiting<P>
where
    W: wire::Spec,
    P: Try<W>,
{
    type Wait = mode::Waiting<P>;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>
        = P::Transaction<'a, 'w, 'd, ID>
    where
        W: 'w,
        'd: 'w,
        'w: 'a;
}

/// Direct receive transaction for one wire identity.
pub struct Guard<ID: Identity, W>(marker::PhantomData<fn() -> (W, ID)>);

impl<ID: Identity, W> Completion for Guard<ID, W> {
    fn complete(self) {
        let Self(marker::PhantomData) = self;
    }
}

/// Owner-bound direct receive transaction for one wire identity.
#[repr(transparent)]
pub struct Transaction<'a, 'w, ID: Identity + 'w, W>
where
    W: wire::Spec + 'w,
    'w: 'a,
{
    wire: &'a mut W::Connection<'w, ID>,
}

impl<'a, 'w, ID: Identity + 'w, W> Transaction<'a, 'w, ID, W>
where
    W: wire::Spec + 'w,
    'w: 'a,
{
    /// Consumes the reservation into its exact borrowed connection.
    pub fn into_wire(self) -> &'a mut W::Connection<'w, ID> {
        self.wire
    }

    pub(crate) fn new(wire: &'a mut W::Connection<'w, ID>) -> Self {
        Self { wire }
    }
}

const _: () = assert!(size_of::<Guard<KeyTag<0>, wire::Identity>>() == 0);
const _: () = assert!(
    size_of::<Transaction<'static, 'static, KeyTag<0>, wire::Identity>>()
        == size_of::<&mut wire::Identity>()
);
