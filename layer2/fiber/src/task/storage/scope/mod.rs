use std::{marker, ops, pin};

use o3::{base::cell::region, depot::arena::slab::key};

mod brand;
mod id;
mod owned;

pub use brand::Brand;
pub use id::Id;
pub(crate) use id::Owner;
pub use owned::Owned;

/// The invariant identity of one exclusive storage access scope.
pub struct Domain<'s, D> {
    scope: marker::PhantomData<fn(&'s mut ()) -> &'s mut ()>,
    domain: marker::PhantomData<fn(D) -> D>,
}

/// A task identity bound to one exclusive storage access scope.
pub type Scoped<'s, 'd, D, K = key::Parts> = Id<'d, Domain<'s, D>, K>;

/// Exclusive access carrying the original storage's invariant identity proof.
pub struct Scope<'s, S> {
    pub(in crate::task::storage) source: S,
    pub(in crate::task::storage) proof: region::Identity<'s>,
}

impl<'s, S> Scope<'s, S> {
    pub(in crate::task::storage) fn new(source: S, proof: region::Identity<'s>) -> Self {
        Self { source, proof }
    }
}

impl<S: Unpin> Scope<'_, S> {
    pub fn as_mut(&mut self) -> pin::Pin<&mut Self> {
        pin::Pin::new(self)
    }

    pub fn as_ref(&self) -> pin::Pin<&Self> {
        pin::Pin::new(self)
    }
}

impl<S> ops::Deref for Scope<'_, S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.source
    }
}

impl Scope<'_, ()> {
    /// Lends one pinned storage instance under a fresh, non-escaping identity.
    pub fn with<S, R>(
        source: pin::Pin<&mut S>,
        run: impl for<'s> FnOnce(Scope<'s, pin::Pin<&'s mut S>>) -> R,
    ) -> R {
        region::Token::scope_pinned(source, |token, source| {
            run(Scope::new(source, token.proof()))
        })
    }
}
