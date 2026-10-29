use std::{marker, ops};

pub(crate) use attempt::{Attempt, State};
pub use direct::Direct;
use dope_core::{
    driver::{
        retained::flight,
        route::{Identity, kind},
    },
    io::transfer,
};
use o3::base::marker::ThreadBound;
pub use policy::Policy;

use crate::wire;

mod sources;
pub use sources::{either::Either, empty::Empty, plain::Plain, vectored::Vectored};
mod attempt;
mod control;
mod direct;
mod policy;
pub mod progress;
/// Unsafe adapters for constructing retained send views from proven owners.
pub mod raw;

pub mod reclaim;

/// Byte count validated against the exact send which produced it.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Sent(transfer::Len);

impl Sent {
    pub(crate) const fn new(bytes: transfer::Len) -> Self {
        Self(bytes)
    }

    pub(crate) const fn into_len(self) -> transfer::Len {
        self.0
    }
}

/// Consumes a bounded completion prefix from one send owner.
pub trait Consume: Store {
    fn consume_max(&mut self, max: transfer::Len);
}

/// Send storage borrowed exclusively while a prepared send retains its bytes.
pub trait Store {
    fn as_slice(&self) -> &[u8];

    /// Consumes the backend and reports whether that returned shared capacity.
    fn release(self) -> Availability;
}

impl Store for () {
    fn as_slice(&self) -> &[u8] {
        let () = self;
        &[]
    }

    fn release(self) -> Availability {
        let () = self;
        Availability::Unchanged
    }
}

pub struct Storage<'a, S: Store> {
    storage: &'a mut S,
    scope: Scope<'a>,
}

struct Scope<'a>(marker::PhantomData<&'a mut ThreadBound>);
/// One kernel completion bound to the exclusive storage transaction that
/// produced it.
#[must_use]
pub struct Completion<'a, P: reclaim::Policy, S: Store> {
    storage: Storage<'a, S>,
    sent: Sent,
    policy: marker::PhantomData<fn() -> P>,
}

/// Plaintext progress issued by the exact admitted input plan.
/// The exclusive send-storage transaction stays paired with that progress.
#[must_use = "consumed storage must be turned into a prepared send"]
pub struct Consumed<'a, S: Store> {
    storage: Storage<'a, S>,
    consumed: transfer::Len,
}

impl<'a, P: reclaim::Policy, S: Store> Completion<'a, P, S> {
    pub(crate) fn from_sent(storage: Storage<'a, S>, sent: Sent) -> Self {
        Self {
            storage,
            sent,
            policy: marker::PhantomData,
        }
    }

    pub fn into_storage(self) -> Storage<'a, S> {
        self.storage
    }
}

impl<'a, S: Consume> Completion<'a, reclaim::OnSubmit, S> {
    /// Advances buffered output without issuing plaintext-consumption credit.
    pub fn consume_max(self) -> Storage<'a, S> {
        Consume::consume_max(self.storage.storage, self.sent.into_len());
        self.storage
    }
}

impl<'a, S: Store> Storage<'a, S> {
    /// Begins one exclusive transaction over connection-local send storage.
    pub fn new(store: &'a mut S) -> Self {
        Self {
            storage: store,
            scope: Scope(marker::PhantomData),
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.storage.as_slice()
    }

    /// Issues the zero-consumption proof without a fallible integer conversion.
    pub fn consume_none(self) -> Consumed<'a, S> {
        Consumed {
            storage: self,
            consumed: transfer::Len::ZERO,
        }
    }

    /// Retains exact owner-backed input through completion while holding this
    /// storage transaction's exclusive lifetime.
    pub fn input(self, plain: Plain<'a>) -> Prepared<'a, reclaim::OnComplete, Plain<'a>> {
        Prepared::new(plain, reclaim::Exact, self.scope)
    }

    /// Retains exact owner-backed descriptors and input through completion
    /// while holding this storage transaction's exclusive lifetime.
    pub fn vectored<const N: usize>(
        self,
        plain: Vectored<'a, N>,
    ) -> Prepared<'a, reclaim::OnComplete, Vectored<'a, N>> {
        Prepared::new(plain, reclaim::Exact, self.scope)
    }

    /// Prepares connection-owned output without consuming caller input.
    pub fn buffered(self) -> Prepared<'a, reclaim::OnSubmit, Plain<'a>> {
        self.consume_none().buffered()
    }

    /// Sends retained output, or waits for shared capacity when it is empty.
    /// This transaction never consumes caller input.
    pub fn buffer_wait(self) -> Prepared<'a, reclaim::OnSubmit, Plain<'a>> {
        let mut ready = self.buffered();
        if ready.payload.is_empty() {
            ready.control.mark_waiting();
        }
        ready
    }

    /// Prepares no output and consumes no caller input.
    pub fn empty<P: reclaim::Policy>(self) -> Prepared<'a, P, Empty> {
        Prepared::new(Empty, P::empty(), self.scope)
    }
}

impl<'a, S: Store> Consumed<'a, S> {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage.storage.as_slice().is_empty()
    }

    /// Prepares connection-owned output with the exact proven consumption.
    pub fn buffered(self) -> Prepared<'a, reclaim::OnSubmit, Plain<'a>> {
        let bytes = self.storage.storage.as_slice();
        Prepared::new(Plain::proven(bytes), self.consumed, self.storage.scope)
    }
}

impl<S: Store> ops::Deref for Storage<'_, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.storage
    }
}

impl<S: Store> ops::DerefMut for Storage<'_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.storage
    }
}

/// Borrowed send representation with an exact bounded byte count.
/// Retaining its addresses past this borrow requires the raw submission contract.
pub trait Payload<'a> {
    fn select(&self) -> Option<impl raw::Ready<'a>>;
}

/// Whether an operation returned shared wire send storage.
#[must_use]
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    /// No shared send storage was returned.
    Unchanged = 0,
    /// Shared send storage was returned.
    Released = 1 << 1,
}

/// Connection action attached to one prepared send.
#[must_use]
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// Keep the write side open after submission.
    Keep,
    /// Drain the write side after submission.
    Close,
}

/// Prepared output scoped to an exclusive send-storage transaction. The
/// storage cannot be borrowed again while this value remains live.
#[must_use]
pub struct Prepared<'a, P: reclaim::Policy, B: Payload<'a>> {
    payload: B,
    credit: P::Credit,
    control: control::Control,
    scope: Scope<'a>,
}

#[must_use]
#[repr(transparent)]
/// Resource transition caused by one send completion.
/// `OnSubmit` may carry independent follow-up output; `OnComplete` can only
/// terminate the exact retained input represented by that completion.
pub struct Transition<'a, P: reclaim::Policy, B: Payload<'a>>(Prepared<'a, P, B>);

impl<'a, B: Payload<'a>> Transition<'a, reclaim::OnSubmit, B> {
    pub fn new(mut ready: Prepared<'a, reclaim::OnSubmit, B>, free: Availability) -> Self {
        ready.control.mark_free(free);
        Self(ready)
    }

    pub fn unchanged(ready: Prepared<'a, reclaim::OnSubmit, B>) -> Self {
        Self::new(ready, Availability::Unchanged)
    }
}

impl<'a> Transition<'a, reclaim::OnComplete, Empty> {
    /// Completes one exact retained input without chaining unrelated output.
    pub fn completed<S: Store>(send: Storage<'a, S>) -> Self {
        Self(send.empty())
    }
}

impl<'a, P: reclaim::Policy, B: Payload<'a>> Transition<'a, P, B> {
    pub const fn availability(&self) -> Availability {
        self.0.control.availability()
    }

    pub fn into_parts(self) -> (Prepared<'a, P, B>, Availability) {
        let free = self.availability();
        (self.0, free)
    }
}

impl<'a, P: reclaim::Policy, B: Payload<'a>> Prepared<'a, P, B> {
    fn new(body: B, credit: P::Credit, scope: Scope<'a>) -> Self {
        Self {
            payload: body,
            credit,
            control: control::Control::new(),
            scope,
        }
    }

    pub fn close_after(mut self) -> Self {
        self.control.mark_close();
        self
    }

    pub(crate) const fn is_waiting(&self) -> bool {
        self.control.is_waiting()
    }

    pub fn into_parts(self) -> (B, P::Credit, End) {
        (self.payload, self.credit, self.control.end())
    }

    /// Embeds this representation in the left arm of an explicit mixed
    /// payload. Policies which always emit one representation should keep the
    /// concrete `Prepared` and avoid this discriminant entirely.
    pub fn into_left<R: Payload<'a>>(self) -> Prepared<'a, P, Either<B, R>> {
        Prepared {
            payload: Either::Left(self.payload),
            credit: self.credit,
            control: self.control,
            scope: self.scope,
        }
    }

    /// Embeds this representation in the right arm of an explicit mixed
    /// payload.
    pub fn into_right<L: Payload<'a>>(self) -> Prepared<'a, P, Either<L, B>> {
        Prepared {
            payload: Either::Right(self.payload),
            credit: self.credit,
            control: self.control,
            scope: self.scope,
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Scope<'static>>() == 0);
    assert!(std::mem::size_of::<Storage<'static, ()>>() == std::mem::size_of::<&'static mut ()>());
    assert!(
        std::mem::align_of::<Storage<'static, ()>>() == std::mem::align_of::<&'static mut ()>()
    );
    assert!(
        std::mem::size_of::<Completion<'static, reclaim::OnSubmit, ()>>()
            == std::mem::size_of::<(Storage<'static, ()>, Sent)>()
    );
};

pub struct Submitted<'d> {
    pub bytes: transfer::NonZero,
    pub flight: flight::Flight<'d, kind::Send>,
}

/// One exact wire and send-state transaction split from the same connection.
#[must_use]
pub struct Context<'a, 'd, ID: Identity + 'd, W: wire::Spec + 'd, T>
where
    'd: 'a,
{
    wire: &'a mut W::Connection<'d, ID>,
    state: T,
}

impl<'a, 'd, ID: Identity + 'd, W: wire::Spec + 'd, T> Context<'a, 'd, ID, W, T>
where
    'd: 'a,
{
    pub(crate) fn new(wire: &'a mut W::Connection<'d, ID>, state: T) -> Self {
        Self { wire, state }
    }

    pub fn into_parts(self) -> (&'a mut W::Connection<'d, ID>, T) {
        (self.wire, self.state)
    }

    /// Releases the wire borrow and returns its exact state.
    pub fn into_state(self) -> T {
        self.state
    }
}
