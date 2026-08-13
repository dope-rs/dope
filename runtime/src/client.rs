//! Runtime-scoped client capabilities.
//!
//! A [`Scope`] can only be issued by the runtime from the exact selected
//! provider. Providers turn that zero-sized capability into their public
//! client handle without retaining the transient selector borrow.
//!
//! The application lifetime cannot escape [`crate::executor::session::Session::with_app`]:
//!
//! ```compile_fail
//! use dope_runtime::{
//!     client::Provider,
//!     executor::{self, Application},
//! };
//!
//! fn escape<'scope, 'd, D>(
//!     session: &mut executor::session::Session<'scope, 'd>,
//!     dispatcher: D,
//! ) -> D::Client<'d>
//! where
//!     'd: 'scope,
//!     D: Application<'d> + Provider<'d>,
//! {
//!     session.with_app(dispatcher, |mut app| app.client(|dispatcher| dispatcher))
//! }
//! ```
//!
//! A scope cannot be constructed outside the runtime:
//!
//! ```compile_fail,E0624
//! use dope_runtime::client::Scope;
//!
//! let _ = Scope::<'static, 'static, ()>::new();
//! ```
//!
//! The provider-erased application lease is equally unforgeable:
//!
//! ```compile_fail,E0624
//! use dope_runtime::client::Lease;
//!
//! let _ = Lease::<'static, 'static>::new();
//! ```
//!
//! A provider cannot turn the selector's transient borrow into an
//! application-scoped reference:
//!
//! ```compile_fail
//! use std::pin::Pin;
//! use dope_runtime::client::{Provider, Scope};
//!
//! struct Borrowing;
//!
//! impl<'d> Provider<'d> for Borrowing {
//!     type Client<'app> = &'app Borrowing where 'd: 'app;
//!
//!     fn provide<'app>(
//!         self: Pin<&Self>,
//!         _scope: Scope<'app, 'd, Self>,
//!     ) -> Self::Client<'app>
//!     where
//!         'd: 'app,
//!     {
//!         self.get_ref()
//!     }
//! }
//! ```
//!
//! The higher-ranked selector likewise cannot save its dispatcher borrow:
//!
//! ```compile_fail
//! use std::pin::Pin;
//! use dope_runtime::{
//!     client::Provider,
//!     executor::{self, Application},
//! };
//!
//! fn transient<'app, 'd, D>(
//!     app: &mut executor::session::Application<'app, 'd, D>,
//! ) -> Pin<&'app D>
//! where
//!     'd: 'app,
//!     D: Application<'d> + Provider<'d>,
//! {
//!     let mut leaked = None;
//!     let _ = app.client(|dispatcher| {
//!         leaked = Some(dispatcher);
//!         dispatcher
//!     });
//!     leaked.unwrap()
//! }
//! ```

use std::{marker, pin};

use crate::executor::session;

/// Address-stable provider root borrowed for one scoped composition.
/// Its private constructor pairs the client scope with this exact root.
#[repr(transparent)]
pub struct Anchor<'app, T> {
    root: pin::Pin<&'app mut T>,
}

impl<'app, T> Anchor<'app, T> {
    pub(crate) fn new(root: pin::Pin<&'app mut T>) -> Self {
        Self { root }
    }

    /// Reborrows the address-stable root mutably.
    #[doc(hidden)]
    pub fn as_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut T> {
        self.get_mut().root.as_mut()
    }

    /// Reborrows the address-stable root immutably.
    #[doc(hidden)]
    pub fn as_ref(self: pin::Pin<&Self>) -> pin::Pin<&T> {
        self.get_ref().root.as_ref()
    }
}

/// Runs one application composition around an address-stable provider root.
/// Neither scoped input can appear in [`Self::Output`].
pub trait Composition<'scope, 'd: 'scope, S, Q, O, P>
where
    P: Provider<'d>,
{
    type Output;

    fn compose<'app>(
        self,
        client: P::Client<'app>,
        root: Anchor<'app, O>,
        session: &mut session::Session<'scope, 'd, S, Q>,
    ) -> Self::Output
    where
        'd: 'app;
}

/// Provider-erased, zero-sized runtime scope proof.
/// It does not claim which provider issued it or remains installed.
pub struct Lease<'app, 'd: 'app> {
    app: marker::PhantomData<*mut &'app ()>,
    driver: marker::PhantomData<*mut &'d ()>,
}

impl<'app, 'd: 'app> Lease<'app, 'd> {
    const fn new() -> Self {
        Self {
            app: marker::PhantomData,
            driver: marker::PhantomData,
        }
    }
}

impl Copy for Lease<'_, '_> {}

impl Clone for Lease<'_, '_> {
    fn clone(&self) -> Self {
        *self
    }
}

/// An unforgeable, zero-sized proof that exact provider type `P` issued a
/// client in runtime scope `'app` and driver scope `'d`.
pub struct Scope<'app, 'd: 'app, P: ?Sized> {
    app: marker::PhantomData<*mut &'app ()>,
    driver: marker::PhantomData<*mut &'d ()>,
    provider: marker::PhantomData<*mut P>,
}

impl<'app, 'd: 'app, P: ?Sized> Scope<'app, 'd, P> {
    pub(crate) const fn new() -> Self {
        Self {
            app: marker::PhantomData,
            driver: marker::PhantomData,
            provider: marker::PhantomData,
        }
    }

    /// Erases the selected provider while preserving the exact application
    /// and driver liveness proof.
    pub const fn lease(self) -> Lease<'app, 'd> {
        Lease::new()
    }

    /// Narrows this proof to the lifetime of a borrow without changing its
    /// provider or driver identity.
    pub const fn reborrow<'short>(&'short self) -> Scope<'short, 'd, P>
    where
        'app: 'short,
    {
        Scope {
            app: marker::PhantomData,
            driver: marker::PhantomData,
            provider: marker::PhantomData,
        }
    }
}

impl<P: ?Sized> Copy for Scope<'_, '_, P> {}

impl<P: ?Sized> Clone for Scope<'_, '_, P> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Produces a client from the exact selected provider.
/// The result may retain only runtime scope, not the selector borrow.
pub trait Provider<'d> {
    type Client<'app>: 'app
    where
        'd: 'app;

    fn provide<'app>(self: pin::Pin<&Self>, scope: Scope<'app, 'd, Self>) -> Self::Client<'app>
    where
        'd: 'app;
}
