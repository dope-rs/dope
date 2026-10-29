use std::{marker, ops, pin};

use o3::base::cell::region;

use crate::{
    core::driver::schedule::{app, ready::task::owner},
    task::storage::fixed::{
        direct, quota,
        schedule::{self, queue, result::Selection},
        tree,
    },
};

mod brand;
mod id;
mod owned;
mod route;
pub use brand::Brand;
pub use id::Id;
pub(crate) use id::Owner;
pub use owned::Owned;
pub use route::Route;

/// The invariant identity of one exclusive storage access scope.
pub struct Domain<'s, D> {
    scope: marker::PhantomData<fn(&'s mut ()) -> &'s mut ()>,
    domain: marker::PhantomData<fn(D) -> D>,
}

/// A task identity bound to one exclusive storage access scope.
pub type Scoped<'s, 'd, D> = Id<'d, Domain<'s, D>>;

/// Exclusive access carrying the original storage's invariant identity proof.
pub struct Scope<'s, S> {
    pub(in crate::task::storage) source: S,
    pub(in crate::task::storage) proof: region::Proof<region::Identity<'s>>,
}

impl<'s, S> Scope<'s, S> {
    pub(in crate::task::storage) fn new(
        source: S,
        proof: region::Proof<region::Identity<'s>>,
    ) -> Self {
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

impl<'s, 'a, 'd, R, Q> Scope<'s, pin::Pin<&'a mut direct::Plane<'d, R, Q>>>
where
    R: tree::Tree<'d>,
    Q: quota::State,
{
    pub fn into_route<P>(self) -> Route<'s, direct::Route<'a, 'd, R, P, Q>>
    where
        R: tree::At<'d, P>,
    {
        Route::new(self.source.route(), self.proof)
    }

    pub fn route<P>(self: pin::Pin<&mut Self>) -> Route<'s, direct::Route<'_, 'd, R, P, Q>>
    where
        R: tree::At<'d, P>,
    {
        let this = self.get_mut();
        Route::new(this.source.as_mut().route(), this.proof)
    }
}

impl<'s, 'a, 'd, R, Q, S> Scope<'s, pin::Pin<&'a mut schedule::Plane<'d, R, Q, S>>>
where
    R: tree::Tree<'d>,
    Q: quota::State,
    S: queue::Backend<'d>,
{
    pub fn into_route<P>(self) -> Route<'s, schedule::Route<'a, 'd, R, P, Q, S>>
    where
        R: tree::At<'d, P>,
    {
        Route::new(self.source.route(), self.proof)
    }

    pub fn route<P>(self: pin::Pin<&mut Self>) -> Route<'s, schedule::Route<'_, 'd, R, P, Q, S>>
    where
        R: tree::At<'d, P>,
    {
        let this = self.get_mut();
        Route::new(this.source.as_mut().route(), this.proof)
    }
}

impl<'s, 'a, 'd, R, Q> Scope<'s, pin::Pin<&'a mut schedule::Plane<'d, R, Q>>>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
{
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, R, Q> {
        self.get_mut().source.as_mut().select(work)
    }
}

impl<'s, 'a, 'd, R, Q, Source>
    Scope<'s, pin::Pin<&'a mut schedule::Plane<'d, R, Q, queue::Owned<'d, Source>>>>
where
    R: tree::Tree<'d>,
    Q: quota::State,
{
    pub fn owners(self: pin::Pin<&Self>) -> queue::Owners<'_, 'd, Source> {
        self.get_ref().source.as_ref().owners()
    }
}

impl<'s, 'a, 'd, R, Q, Source>
    Scope<'s, pin::Pin<&'a mut schedule::Plane<'d, R, Q, queue::Owned<'d, Source>>>>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    Source: Copy,
{
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        scope: owner::Scope<Source>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, R, Q, queue::Owned<'d, Source>> {
        self.get_mut().source.as_mut().select(scope, work)
    }

    pub fn wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) {
        self.get_mut().source.as_mut().wake(scope);
    }

    pub fn try_wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) -> queue::Wake {
        self.get_mut().source.as_mut().try_wake(scope)
    }
}
