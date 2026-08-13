use std::{error, fmt, marker, time};

use dope_core::io::socket::{self, option};
use dope_net::link::pool;
use o3::collections::slab::key;

use crate::connector::lifecycle;

pub mod queue;

mod contract;
pub(crate) use contract::Contract;

/// Identity of one frozen transport connection attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Id<'d, const ID: u8 = 0> {
    parts: key::Parts,
    _region: marker::PhantomData<*mut &'d ()>,
}

const _: () = assert!(std::mem::size_of::<Id<'static>>() == std::mem::size_of::<key::Parts>());

impl<const ID: u8> Id<'_, ID> {
    pub(crate) const fn from_generation(index: u32, generation: key::Generation) -> Self {
        Self {
            parts: key::Parts::from_generation(index, generation),
            _region: marker::PhantomData,
        }
    }

    pub(crate) const fn from_slab<Source>(key: key::Handle<Source>) -> Self {
        Self {
            parts: key.parts(),
            _region: marker::PhantomData,
        }
    }

    pub const fn index(self) -> u32 {
        self.parts.index()
    }

    pub(crate) const fn generation(self) -> key::Generation {
        self.parts.generation()
    }

    pub(crate) const fn parts(self) -> key::Parts {
        self.parts
    }
}

pub enum Action<'d, T: dope_net::Transport, const ID: u8 = 0> {
    Connect { key: Id<'d, ID>, plan: Plan<T> },
    Backoff { min_retry_at: time::Instant },
    Idle,
}

/// A linear, owned kernel connection plan.
/// Submission consumes the resolved address or returns the plan through
/// [`Control::connect_deferred`].
#[doc(hidden)]
pub struct Plan<T: dope_net::Transport> {
    target: Option<(socket::Addr, option::StreamOptions)>,
    _transport: marker::PhantomData<fn() -> T>,
}

impl<T: dope_net::Transport> Plan<T> {
    pub(crate) fn new(addr: &T::Addr, options: option::StreamOptions) -> Self {
        Self {
            target: T::to_sock_addr(addr).ok().map(|addr| (addr, options)),
            _transport: marker::PhantomData,
        }
    }

    pub(crate) fn socket(&self) -> Option<socket::StreamSpec> {
        self.target
            .as_ref()
            .and_then(|(peer, _)| socket::StreamSpec::for_peer(peer).ok())
    }

    pub(crate) fn into_parts(self) -> (Option<socket::Addr>, Option<option::StreamOptions>) {
        match self.target {
            Some((target, options)) => (Some(target), Some(options)),
            None => (None, None),
        }
    }
}

/// A peer paired with an already validated, owned kernel tuning plan.
///
/// An unchecked transport configuration cannot enter the attempt queue:
///
/// ```compile_fail,E0308
/// use std::net::SocketAddr;
/// use dope_manifold::connector::attempt::StreamTarget;
/// use dope_net::tcp::StreamConfig;
///
/// fn unchecked(addr: SocketAddr) {
///     let _ = StreamTarget::new(addr, StreamConfig::default());
/// }
/// ```
pub struct StreamTarget<P> {
    pub(crate) peer: P,
    pub(crate) options: option::StreamOptions,
}

impl<P> StreamTarget<P> {
    pub const fn new(peer: P, options: option::StreamOptions) -> Self {
        Self { peer, options }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    Applied,
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResizeError {
    requested: usize,
    available: usize,
}

impl ResizeError {
    pub(crate) const fn new(requested: usize, available: usize) -> Self {
        Self {
            requested,
            available,
        }
    }
}

impl fmt::Display for ResizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "connector controller capacity {} is below requested {}",
            self.available, self.requested
        )
    }
}

impl error::Error for ResizeError {}

/// Library-owned connector scheduling state machine.
/// This SPI is sealed so linear `Plan` rollback and terminal transitions stay
/// paired with the built-in queue or service pool.
#[doc(hidden)]
pub trait Control<'d, T: dope_net::Transport, const ID: u8 = 0>: Contract {
    fn resize(&mut self, max_connections: usize) -> Result<(), ResizeError>;
    fn poll_connect(&mut self, now: time::Instant) -> Action<'d, T, ID>;
    fn has_pending(&self) -> bool {
        true
    }
    fn connect_succeeded(&mut self, key: Id<'d, ID>, now: time::Instant) -> Transition;
    fn connect_failed(&mut self, key: Id<'d, ID>, now: time::Instant);
    fn connect_options(&mut self, key: Id<'d, ID>) -> Option<option::StreamOptions> {
        let _ = key;
        None
    }
    fn connect_deferred(&mut self, key: Id<'d, ID>, plan: Plan<T>, now: time::Instant) {
        let _ = (key, plan, now);
    }
    fn disconnect(&mut self, key: Id<'d, ID>, reason: lifecycle::CloseReason, now: time::Instant);
    fn kill(&mut self, key: Id<'d, ID>);
    fn bind(
        &mut self,
        key: Id<'d, ID>,
        local: pool::Key<'d, ID>,
        options: Option<option::StreamOptions>,
    ) {
        let _ = (key, local, options);
    }
    fn take_cancel(&mut self) -> Option<(Id<'d, ID>, pool::Key<'d, ID>)> {
        None
    }
}
