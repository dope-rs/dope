use std::{marker, mem, num, ptr};

pub(crate) use contract::Contract;
use dope_core::{
    driver::{
        result,
        route::{Identity, KeyTag, indices},
    },
    io::socket::{self, Prepared as _, option},
};
use dope_net::{link::pool::change::opening, tcp};
use o3::{
    base::{cell::region, num::bounded},
    depot::{arena::slab::key, capacity},
};
pub(crate) use report::Report;

use crate::client::attempt::queue::{storage, table};

/// Attempt ownership classifiers used by connector control implementations.
pub mod mode;
pub mod queue;

mod contract;

mod report;

/// Identity of one frozen transport connection attempt.
#[derive(Debug)]
pub struct Tag<'d, ID: Identity>(marker::PhantomData<fn(region::Identity<'d>) -> ID>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Id<'d, ID: Identity = KeyTag<0>> {
    handle: storage::Key<Tag<'d, ID>>,
}

impl<ID: Identity> Id<'_, ID> {
    pub(crate) fn from_epoch(slot: Slot, epoch: key::Generation) -> Self {
        Self {
            handle: storage::Key::new(bounded::U32::from_one(slot.position()), epoch),
        }
    }

    pub const fn generation(self) -> key::Generation {
        self.handle.generation()
    }

    pub const fn index(self) -> bounded::U32<0, { u32::MAX - 1 }> {
        self.handle.index()
    }
}

pub enum Action<'d, T: dope_net::Transport, ID: Identity = KeyTag<0>> {
    Connect { key: Id<'d, ID>, plan: Plan<T> },
    Idle,
}

/// A linear, owned kernel connection plan.
/// Submission consumes the resolved address or returns the plan to its source.
pub struct Plan<T: dope_net::Transport> {
    target: T::Prepared,
    options: option::Set,
}

impl<T: dope_net::Transport> Plan<T> {
    pub(crate) fn new(addr: &T::Addr, opts: option::Set) -> Self {
        Self {
            target: T::Prepared::from(addr),
            options: opts,
        }
    }

    pub(crate) fn from_target(target: StreamTarget<T>) -> Self {
        Self {
            target: target.peer,
            options: target.options,
        }
    }

    pub(crate) fn socket(&self) -> socket::StreamSpec {
        self.target.stream_spec()
    }

    pub(crate) fn parts(&self) -> (T::Prepared, option::Set) {
        (self.target, self.options)
    }
}

/// A peer paired with an already validated, owned kernel tuning plan.
///
/// An unchecked transport configuration cannot enter the attempt queue.
pub struct StreamTarget<T: dope_net::Transport> {
    pub(crate) peer: T::Prepared,
    pub(crate) options: option::Set,
}

impl<T: dope_net::Transport> StreamTarget<T> {
    pub fn new(peer: T::Addr, opts: option::Set) -> Self {
        Self {
            peer: T::Prepared::from(&peer),
            options: opts,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transition {
    Applied,
    Stale,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Connection<R = ()> {
    Applied(R),
    Stale,
    Disabled,
    Retired,
    Exhausted,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ResizeError {
    requested: capacity::Count,
    available: capacity::Count,
}

impl ResizeError {
    pub(crate) const fn new(need: capacity::Count, avail: capacity::Count) -> Self {
        Self {
            requested: need,
            available: avail,
        }
    }

    pub const fn requested(self) -> capacity::Count {
        self.requested
    }

    pub const fn available(self) -> capacity::Count {
        self.available
    }
}

/// Library-owned connector scheduling state machine.
/// This SPI is sealed so linear `Plan` rollback and terminal transitions stay
/// owned by the built-in queue or service pool.
pub trait Control<'d, T: dope_net::Transport, ID: Identity = KeyTag<0>>: Contract {
    type Mode: mode::Mode;
    type Error;

    fn resize(&mut self, cap: capacity::Count) -> Result<(), ResizeError>;
    fn is_pending(&self) -> bool;
    fn connected(&mut self, key: Id<'d, ID>) -> Result<Connection, Self::Error>;
    /// Records terminal admission for a live owner and returns its exact error.
    /// The owner remains active until its actual retirement.
    fn rejected(&mut self, key: Id<'d, ID>, error: result::SubmitError) -> result::SubmitError;
    fn kill(
        &mut self,
        key: Id<'d, ID>,
        cause: Option<opening::Exhaustion>,
    ) -> Result<(), Self::Error>;
}

pub(crate) trait Peer<'d, T: dope_net::Transport, ID: Identity = KeyTag<0>>:
    Control<'d, T, ID>
{
    type Target;

    fn attach<R>(&mut self, key: Id<'d, ID>, map: impl FnOnce(&Self::Target) -> R)
    -> Connection<R>;
}

const _: () = {
    assert!(mem::size_of::<Slot>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<Slot>>() == mem::size_of::<Slot>());
    assert!(mem::size_of::<Id<'static>>() == mem::size_of::<key::Parts>());
    assert!(mem::align_of::<Id<'static>>() == mem::align_of::<key::Parts>());
    assert!(mem::size_of::<Option<Id<'static>>>() == mem::size_of::<Id<'static>>());
    assert!(mem::size_of::<Connection>() == 1);
    assert!(mem::size_of::<Option<Connection>>() == 1);
};
const _: () = assert!(
    mem::size_of::<Plan<tcp::Tcp>>()
        == mem::size_of::<(<tcp::Tcp as dope_net::Transport>::Prepared, option::Set)>()
);
const _: () = assert!(
    mem::size_of::<StreamTarget<tcp::Tcp>>()
        == mem::size_of::<(<tcp::Tcp as dope_net::Transport>::Prepared, option::Set)>()
);

/// An attempt identity retained with the exact source that reported it.
///
/// The source borrow prevents its storage from moving or being reclaimed while
/// this receipt exists. A copied attempt identifier cannot create a receipt.
///
/// ```compile_fail
/// use dope_manifold::client::attempt::{Completion, queue::Lease};
/// use dope_net::tcp::Tcp;
///
/// fn escape<'source, 'd>(lease: &Lease<'source, 'd, Tcp>)
///     -> Completion<'static, 'd>
/// {
///     lease.receipt()
/// }
/// ```
pub struct Completion<'source, 'd, ID: Identity = KeyTag<0>> {
    key: Id<'d, ID>,
    source: Option<ptr::NonNull<()>>,
    owner: marker::PhantomData<&'source ()>,
}

impl<'source, 'd, ID: Identity> Completion<'source, 'd, ID> {
    pub(in crate::client) fn reported<C: Contract>(key: Id<'d, ID>, control: &'source C) -> Self {
        Self {
            key,
            source: control.identity(),
            owner: marker::PhantomData,
        }
    }

    fn queued<T: dope_net::Transport>(
        key: Id<'d, ID>,
        source: &'source table::Table<'d, T, ID>,
    ) -> Self {
        Self {
            key,
            source: Some(ptr::NonNull::from(source).cast()),
            owner: marker::PhantomData,
        }
    }

    pub const fn id(&self) -> Id<'d, ID> {
        self.key
    }

    pub(in crate::client) fn with_id(self, key: Id<'d, ID>) -> Self {
        Self { key, ..self }
    }

    fn belongs_to<T: dope_net::Transport>(&self, source: &table::Table<'d, T, ID>) -> bool {
        self.source == Some(ptr::NonNull::from(source).cast())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Slot(num::NonZero<u32>);

impl Slot {
    pub(crate) fn new(index: indices::SlotIndex) -> Self {
        Self(index.into())
    }

    pub(crate) const fn position(self) -> num::NonZero<u32> {
        self.0
    }
}
