use o3::{alloc, base::num::bounded};

use crate::{Errno, engine::admit::Submitted, probe};

pub mod flags;
pub mod message;
pub mod raw;
pub mod resolve;

pub mod packet;

use crate::table::token::host;

type Header<E> = Parts<<E as Entry>::Token, <E as Entry>::Outcome, <E as Entry>::Flags>;

/// Owns one dequeued completion and its optional affine buffer.
pub trait Packet: Sized {
    type Parts: Copy;
    type Buffer;

    fn new(parts: Self::Parts, buffer: Option<Self::Buffer>) -> Self;
    fn try_new<E>(
        parts: Self::Parts,
        buffer: Result<Option<Self::Buffer>, E>,
    ) -> Result<Self, probe::Stopped<Self, packet::Invalid<E>>>;
    fn parts(&self) -> Self::Parts;
    fn buffer(&self) -> &Option<Self::Buffer>;
    fn buffer_mut(&mut self) -> &mut Option<Self::Buffer>;
    fn into_parts(self) -> (Self::Parts, Option<Self::Buffer>);
    fn is_plain(&self) -> bool;
    fn flow(&self) -> flags::Flow;
    /// Retires the bound authority or borrows this exact refused packet.
    fn try_retire(&self, echo: impl host::Echo) -> Result<(), &Self>;
}

/// Preserves the typed inputs of an emulated completion before rejection.
pub trait Capture<E, S, F>: Sized {
    fn operation(echo: E) -> Self;
    fn creation(echo: E, queued: S, resolved: Option<S>) -> Self;
    fn receive(echo: E, len: usize, flow: F) -> Self;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decode<V, I> {
    Value(V),
    Error(Errno),
    Invalid(I),
}

/// A completed owner routed to delivery or mandatory reclamation.
pub enum Dequeued<E> {
    Emit(E),
    Reclaim(E),
}

impl<E> Dequeued<E> {
    pub fn map<U>(self, map: impl FnOnce(E) -> U) -> Dequeued<U> {
        match self {
            Self::Emit(event) => Dequeued::Emit(map(event)),
            Self::Reclaim(event) => Dequeued::Reclaim(map(event)),
        }
    }
}

impl<M: Map> Map for Dequeued<M> {
    type Entry<T> = Dequeued<M::Entry<T>>;

    fn map<T, U>(entry: Self::Entry<T>, map: impl FnOnce(T) -> U) -> Self::Entry<U> {
        entry.map(|done| M::map(done, map))
    }
}

/// A borrowed completion queue retaining its native publication order.
pub trait Drain {
    type Entry: Entry;

    fn acquire(&mut self);
    fn publish(&mut self);
    fn refresh(&mut self);
    fn len(&self) -> bounded::U32<0, { u32::MAX }>;
    fn is_empty(&self) -> bool;
    fn pop(&mut self) -> Option<Self::Entry>;
}

/// Inspects the next published completion without advancing either queue head.
/// The returned borrow keeps the observed slot stable until an infallible commit.
pub trait Inspect: Drain {
    type Head<'a>: Head<Entry = Self::Entry>
    where
        Self: 'a;

    fn front(&mut self) -> Option<Self::Head<'_>>;
    fn available(&self) -> bounded::U32<0, { u32::MAX }>;
}

/// A published, unconsumed completion tied to the queue's exclusive borrow.
pub trait Head: Sized {
    type Entry: Entry;

    fn parts(&self) -> Header<Self::Entry>
    where
        <Self::Entry as Entry>::Token: raw::Token;

    /// Advances and publishes the head after every semantic check has succeeded.
    fn commit(self);
}

pub trait Entry: Sized {
    type Token;
    type Outcome: Outcome;
    type Flags: Flags;

    fn into_parts(self) -> Parts<Self::Token, Self::Outcome, Self::Flags>
    where
        Self::Token: raw::Token;
}

pub trait Event {
    type Key: Copy + Eq;

    fn token(&self) -> Option<Self::Key>;
    fn is_terminal(&self) -> bool;
}

pub trait Map {
    type Entry<T>;

    fn map<T, U>(entry: Self::Entry<T>, map: impl FnOnce(T) -> U) -> Self::Entry<U>;
}

/// Exact native completion evidence retained by a host decoder.
pub trait Observe<T: Copy + Eq = (), S: Copy + Eq = (), F: Copy + Eq = ()> {
    type Observation: Copy + Eq;
}

/// Separates successful values, native errors, and invalid kernel results.
pub trait Outcome: Copy + Eq {
    type Value: Value;
    type Invalid: Copy + Eq;
    type Returned: crate::kernel::Returned<Value = Self::Value, Invalid = Self::Invalid>;

    fn decode(self) -> Decode<Self::Value, Self::Invalid>;
    fn is_zero(self) -> bool;
    /// Tests the native errno range without extracting its error payload.
    fn is_error(self) -> bool;
    fn canceled(self) -> Result<(), Self::Returned>;
    fn bytes(self) -> Result<Result<Self::Value, crate::Errno>, Self::Returned>;
    fn unit(self) -> Result<Result<(), crate::Errno>, Self::Returned>;
    fn shutdown(self) -> Result<(), Self::Returned>;
    fn stopped(self) -> Result<(), Self::Returned>;
    fn aborted(self) -> Result<(), Self::Returned>;
}

/// Exact native completion fields retained before semantic interpretation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct Parts<T, O, F> {
    pub token: T,
    pub outcome: O,
    pub flags: F,
}

pub trait Queue: Sized {
    type Event: Event;
    type Capacity: raw::Capacity;
    type Vacancy<'a>: Vacancy<Event = Self::Event>
    where
        Self: 'a;

    fn try_new(capacity: Self::Capacity) -> Result<Self, alloc::Error>;

    fn vacant(&mut self) -> Option<Self::Vacancy<'_>>;

    fn pop_front(&mut self) -> Option<Dequeued<Self::Event>>;

    fn suppress(&mut self, key: <Self::Event as Event>::Key);

    fn is_terminal(&self, key: <Self::Event as Event>::Key) -> bool;

    fn is_empty(&self) -> bool;

    fn available(&self) -> usize;
}

/// Preserves the exact native completion fields before semantic rejection.
pub trait Record<T>: Sized {
    type Outcome;
    type Flags;

    fn new(token: T, outcome: Self::Outcome, flags: Self::Flags) -> Self;
}

pub trait Source {
    type Queue<E: Event, C: raw::Capacity>: Queue<Event = E, Capacity = C>;
}

pub trait Vacancy: Sized {
    type Event: Event;

    fn push(self, event: Self::Event);
}

/// Owns one registered operation and its validated native cancellation changes.
pub trait Cancellation: Sized {
    type Event: Event;

    fn commit(self, vacant: impl Vacancy<Event = Self::Event>) -> Submitted;
}

/// A successful kernel result preserving its nonnegative signed range.
pub trait Value: Copy + Eq + Into<bounded::U32<0, { i32::MAX as u32 }>> {
    fn is_zero(self) -> bool;
}

/// Retains both decoded metadata and the exact rejected native flags.
pub trait Flags: Copy + Eq + From<Self::Meta> {
    type Meta: flags::Meta;
    type Invalid: Copy + Eq + Into<Self>;

    fn meta(self) -> Self::Meta;
    fn decode(self) -> Result<Self::Meta, Self::Invalid>;
    fn is_valid(self) -> bool;
}
