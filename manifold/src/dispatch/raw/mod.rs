//! Raw retained owners can be driven only after exact-root installation:
//!
//! ```compile_fail,E0133
//! use std::pin::Pin;
//! use dope_core::driver;
//! use dope_manifold::dispatch::raw::Manifold;
//!
//! fn bypass_install<'d, M: Manifold<'d>>(
//!     owner: Pin<&mut M>,
//!     driver: &mut driver::Context<'_, 'd>,
//! ) {
//!     M::pre_park(owner, driver);
//! }
//! ```

use std::{marker, ops, pin};

use dope_core::{
    driver::{self, lifecycle, retained, route, schedule},
    io,
};
use o3::cell::region;

use crate::dispatch::typed;

mod policy;

pub(crate) use policy::Policy;

/// Compile-time driver authority selected independently for each lifecycle
/// callback.
#[doc(hidden)]
pub trait Capability: Policy {}

/// Ordinary driver access. Retained submissions are not expressible through
/// a context carrying this marker.
#[doc(hidden)]
pub struct Plain;

/// Driver access authorized to retain storage owned by the installed app.
#[doc(hidden)]
pub struct Retained;

impl Policy for Plain {}
impl Policy for Retained {}
impl Capability for Plain {}
impl Capability for Retained {}

/// A zero-cost, statically narrowed driver context.
///
/// Plain callbacks cannot call the retained submission boundary:
///
/// ```compile_fail
/// use dope_core::{
///     driver::{self, retained, route::KeyTag},
/// };
/// use dope_manifold::dispatch::raw::{Context, Plain};
///
/// unsafe fn retain<'a, 'owner, 'd: 'owner>(
///     context: &mut Context<'a, 'owner, 'd, Plain>,
///     slots: &driver::flight::Slots<'d, KeyTag<1>>,
///     submission: retained::raw::Submission<'owner, 'd, KeyTag<1>>,
/// ) {
///     driver::retained::raw::Owner::submit(context, slots, submission).unwrap();
/// }
/// ```
///
/// Narrowing is one-way; a plain callback cannot recover retained authority:
///
/// ```compile_fail
/// use dope_manifold::dispatch::raw::{Context, Plain, Retained};
///
/// fn escalate<'a, 'owner, 'd: 'owner>(
///     context: &mut Context<'a, 'owner, 'd, Plain>,
/// ) {
///     let _ = context.narrow::<Retained>();
/// }
/// ```
#[doc(hidden)]
#[repr(transparent)]
pub struct Context<'a, 'owner, 'd: 'owner, A: Capability> {
    inner: retained::Context<'a, 'owner, 'd>,
    _access: marker::PhantomData<A>,
}

impl<'a, 'owner, 'd: 'owner> Context<'a, 'owner, 'd, Retained> {
    /// Constructs the root retained view after exact app installation.
    pub fn new(inner: retained::Context<'a, 'owner, 'd>) -> Self {
        Self {
            inner,
            _access: marker::PhantomData,
        }
    }

    /// Narrows retained authority to the exact capability declared by one
    /// statically selected manifold callback.
    pub fn narrow<A: Capability>(&mut self) -> Context<'_, 'owner, 'd, A> {
        Context {
            inner: self.inner.reborrow(),
            _access: marker::PhantomData,
        }
    }
}

impl<'a, 'owner, 'd: 'owner> ops::Deref for Context<'a, 'owner, 'd, Plain> {
    type Target = driver::Context<'a, 'd>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, 'owner, 'd: 'owner> ops::DerefMut for Context<'a, 'owner, 'd, Plain> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'a, 'owner, 'd: 'owner> ops::Deref for Context<'a, 'owner, 'd, Retained> {
    type Target = retained::Context<'a, 'owner, 'd>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, 'owner, 'd: 'owner> ops::DerefMut for Context<'a, 'owner, 'd, Retained> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Context<'static, 'static, 'static, Plain>>()
            == std::mem::size_of::<driver::Context<'static, 'static>>()
    );
    assert!(
        std::mem::align_of::<Context<'static, 'static, 'static, Plain>>()
            == std::mem::align_of::<driver::Context<'static, 'static>>()
    );
    assert!(
        std::mem::size_of::<Context<'static, 'static, 'static, Retained>>()
            == std::mem::size_of::<retained::Context<'static, 'static, 'static>>()
    );
    assert!(
        std::mem::align_of::<Context<'static, 'static, 'static, Retained>>()
            == std::mem::align_of::<retained::Context<'static, 'static, 'static>>()
    );
};

/// # Safety
/// The owner remains pinned through `shutdown` and `finish`; deferred events
/// and borrowed lifecycle contexts may not escape their dispatch call.
#[doc(hidden)]
pub unsafe trait Manifold<'d>: Sized {
    const ID: u8;

    type Dispatch: Capability;
    type Activate: Capability;
    type PrePark: Capability;
    type Shutdown: Capability;

    /// Installs a staged retained owner only after its exact dispatcher root
    /// has been pinned under runtime lifecycle control.
    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        let _ = install;
    }

    /// # Safety
    /// `token.route()` must equal `Self::ID`.
    #[doc(hidden)]
    unsafe fn token_from_route_unchecked(token: route::Token) -> typed::Token<'d, Self> {
        debug_assert_eq!(token.route(), Self::ID);
        typed::Token(token, marker::PhantomData)
    }

    /// # Safety
    /// The enclosing exact dispatcher must have installed this pinned owner
    /// and must retain it through shutdown and finish.
    unsafe fn dispatch<'turn>(
        self: pin::Pin<&mut Self>,
        ev: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let _ = (ev, turn, driver);
        ops::ControlFlow::Continue(())
    }

    /// # Safety
    /// The enclosing exact dispatcher must have installed this pinned owner.
    unsafe fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut Context<'_, '_, 'd, Self::PrePark>,
    );

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let _ = region;
        schedule::Progress::Quiescent
    }

    fn shutdown_progress(
        self: pin::Pin<&Self>,
        region: &region::Token<'d>,
    ) -> schedule::Progress<'d> {
        self.progress(region)
    }

    /// # Safety
    /// The enclosing exact dispatcher must have installed this pinned owner.
    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        target: typed::Token<'d, Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut Context<'_, '_, 'd, Self::Activate>,
    ) {
        let _ = (target, turn, driver);
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut Context<'_, '_, 'd, Self::Shutdown>,
    );

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        let _ = finish;
    }
}

/// Restricted coordination view of one installed Manifold.
/// # Safety
/// `Control` cannot move or release owner-backed state and stays inside `'step`.
#[doc(hidden)]
pub unsafe trait Controlled<'d>: Manifold<'d> {
    type Control<'step>
    where
        Self: 'step,
        'd: 'step;

    /// # Safety
    /// The exact installed root is exclusive until the returned control drops.
    unsafe fn control<'step>(self: pin::Pin<&'step mut Self>) -> Self::Control<'step>
    where
        'd: 'step;
}
