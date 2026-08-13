use std::{fmt, hash, marker, mem};

use dope_core::driver::route;
use dope_net::link::pool;

pub(crate) mod arms;
pub mod identity;

type Invariant<'d, M> = marker::PhantomData<(fn(&'d ()) -> &'d (), *mut M)>;

#[repr(transparent)]
pub struct Token<'d, M>(pub(crate) route::Token, pub(crate) Invariant<'d, M>);

const _: () = assert!(mem::size_of::<Token<'static, ()>>() == mem::size_of::<route::Token>());
const _: () = assert!(mem::align_of::<Token<'static, ()>>() == mem::align_of::<route::Token>());

impl<M> Clone for Token<'_, M> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M> Copy for Token<'_, M> {}

impl<'d, M> Token<'d, M> {
    /// Retags a token while statically proving that both manifolds own the
    /// same runtime route.
    ///
    /// This is the zero-cost forwarding operation for transparent manifold
    /// wrappers. A differing pair of route IDs fails during monomorphization
    /// rather than adding a runtime branch.
    ///
    /// ```
    /// use dope_manifold::{timing::interval::Interval, dispatch::typed::Token};
    ///
    /// type Outer = Interval<'static, 7>;
    /// type Inner = Interval<'static, 7>;
    ///
    /// fn forward(token: Token<'static, Outer>) -> Token<'static, Inner> {
    ///     token.retag::<Inner>()
    /// }
    ///
    /// fn main() {
    ///     let _ = forward as fn(Token<'static, Outer>) -> Token<'static, Inner>;
    /// }
    /// ```
    ///
    /// ```compile_fail,E0080
    /// use dope_manifold::{timing::interval::Interval, dispatch::typed::Token};
    ///
    /// type Outer = Interval<'static, 7>;
    /// type OtherRoute = Interval<'static, 8>;
    ///
    /// fn cross_route(token: Token<'static, Outer>) -> Token<'static, OtherRoute> {
    ///     token.retag::<OtherRoute>()
    /// }
    ///
    /// fn main() {
    ///     let _ = cross_route
    ///         as fn(Token<'static, Outer>) -> Token<'static, OtherRoute>;
    /// }
    /// ```
    ///
    /// The driver brand cannot be extended or replaced.
    ///
    /// ```compile_fail
    /// use dope_manifold::dispatch::typed::Token;
    ///
    /// fn escape<'d, M>(token: Token<'d, M>) -> Token<'static, M> {
    ///     token
    /// }
    /// ```
    pub const fn retag<N>(self) -> Token<'d, N>
    where
        M: crate::dispatch::raw::Manifold<'d>,
        N: crate::dispatch::raw::Manifold<'d>,
    {
        const {
            assert!(
                M::ID == N::ID,
                "cannot retag a token across different manifold routes"
            )
        };
        Token(self.0, marker::PhantomData)
    }

    pub(crate) const fn raw(self) -> route::Token {
        self.0
    }
}

/// A generation-checked connection identity branded by driver lifetime,
/// route, and connection family.
#[repr(transparent)]
pub(crate) struct Id<'d, const ID: u8, Kind> {
    pub(crate) key: pool::Key<'d, ID>,
    _brand: marker::PhantomData<fn(&'d Kind) -> &'d Kind>,
}

impl<const ID: u8, Kind> Copy for Id<'_, ID, Kind> {}

impl<const ID: u8, Kind> Clone for Id<'_, ID, Kind> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<const ID: u8, Kind> PartialEq for Id<'_, ID, Kind> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<const ID: u8, Kind> Eq for Id<'_, ID, Kind> {}

impl<const ID: u8, Kind> hash::Hash for Id<'_, ID, Kind> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<const ID: u8, Kind> fmt::Debug for Id<'_, ID, Kind> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionId")
            .field("route", &ID)
            .field("slot", &self.key.lane())
            .field("epoch", &self.key.epoch())
            .finish()
    }
}

impl<'d, const ID: u8, Kind> Id<'d, ID, Kind> {
    pub(crate) fn from_key(key: pool::Key<'d, ID>) -> Self {
        Self {
            key,
            _brand: marker::PhantomData,
        }
    }
}

const _: () = assert!(mem::size_of::<Id<'static, 0, ()>>() == mem::size_of::<route::Token>());
const _: () = assert!(mem::align_of::<Id<'static, 0, ()>>() == mem::align_of::<route::Token>());
