//! Raw retained owners can be driven only after exact-root installation.

use std::{marker, mem, ops, pin};

use dope_core::{
    driver::{
        self,
        result::SubmitError,
        retained::{self, flight},
        route::{self, kind},
        schedule,
    },
    io::fs,
};
use dope_runtime::executor;

use crate::app::dispatch::event;

mod activate;
mod call;
mod omit;
mod probe;
mod resume;
mod wakes;

pub use activate::Activate;
pub use call::Call;
pub use omit::Omit;
pub use probe::Probe;
pub use resume::Resume;
pub use wakes::Wakes;

/// Ordinary driver access. Retained submissions are not expressible through
/// a context carrying this marker.
pub struct Plain;

/// Driver access authorized to retain storage owned by the installed app.
pub struct Retained;

/// A statically narrowed driver context.
///
/// Plain callbacks cannot call the retained submission boundary:
///
/// ```compile_fail
/// use dope_core::{
///     driver::{self, retained, route},
/// };
/// use dope_manifold::app::dispatch::raw;
///
/// unsafe fn retain<'a, 'owner, 'd: 'owner>(
///     context: &mut raw::Context<'a, 'owner, 'd, raw::Plain>,
///     slots: &driver::retained::flight::Slots<'d, route::KeyTag<1>>,
///     submission: retained::raw::Submission<'owner, 'd, route::KeyTag<1>>,
/// ) {
///     submission.try_submit(context, slots);
/// }
/// ```
///
/// Narrowing is one-way; a plain callback cannot recover retained authority:
///
/// ```compile_fail
/// use dope_manifold::app::dispatch::raw;
///
/// fn escalate<'a, 'owner, 'd: 'owner>(
///     context: &mut raw::Context<'a, 'owner, 'd, raw::Plain>,
/// ) {
///     context.narrow::<raw::Retained>();
/// }
/// ```
#[repr(transparent)]
pub struct Context<'a, 'owner, 'd: 'owner, A> {
    inner: retained::Context<'a, 'owner, 'd>,
    access: marker::PhantomData<A>,
}

impl<'a, 'owner, 'd: 'owner> Context<'a, 'owner, 'd, Retained> {
    /// Constructs the root retained view after exact app installation.
    pub fn new(inner: retained::Context<'a, 'owner, 'd>) -> Self {
        Self {
            inner,
            access: marker::PhantomData,
        }
    }

    /// Narrows retained authority to the exact capability declared by one
    /// statically selected manifold callback.
    pub fn narrow<A>(&mut self) -> Context<'_, 'owner, 'd, A> {
        Context {
            inner: self.inner.reborrow(),
            access: marker::PhantomData,
        }
    }

    /// Reborrows the same exact retained app authority.
    pub fn reborrow(&mut self) -> Context<'_, 'owner, 'd, Retained> {
        Context {
            inner: self.inner.reborrow(),
            access: marker::PhantomData,
        }
    }

    /// # Safety
    /// The exact pinned owner must retain every resource borrowed by `entry`
    /// through terminal completion or driver quiescence.
    pub unsafe fn submit_file<'borrow, Tag, C>(
        self,
        slots: &flight::Slots<'d, Tag>,
        entry: fs::Submission<'borrow, 'd, Tag, C>,
    ) -> Result<flight::Flight<'d, C>, SubmitError>
    where
        'owner: 'borrow,
        Tag: route::Tag,
        C: kind::Flight,
    {
        // SAFETY: the caller retains every borrowed resource in the exact
        // installed owner through terminal completion or driver quiescence.
        unsafe { retained::raw::Submission::try_file(self.inner, slots, entry) }
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

/// Installation behavior owned by a pinned value with a real setup event.
pub trait Install<'d> {
    fn install(self: pin::Pin<&mut Self>);
}

/// Statically selected installation capability for one manifold.
pub trait Installs<'d, M> {
    fn call<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, M>,
    ) -> executor::raw::Install<'app, 'owner, 'd, M>;
}

/// Pinned field projection used to compose inner lifecycle capabilities.
pub trait Projection<'d> {
    type Inner;

    fn inner(self: pin::Pin<&Self>) -> pin::Pin<&Self::Inner>;

    fn inner_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Inner>;
}

/// Installation capability inherited through one pinned field projection.
pub struct Projected<M>(marker::PhantomData<fn() -> M>);

impl<'d, M> Installs<'d, M> for Projected<M>
where
    M: Projection<'d>,
    M::Inner: Manifold<'d>,
{
    fn call<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, M>,
    ) -> executor::raw::Install<'app, 'owner, 'd, M> {
        let (mut owner, proof) = install.into_parts();
        let child = executor::raw::Install::new(Projection::inner_mut(owner.as_mut()), proof);
        let proof = <<M::Inner as Manifold<'d>>::Install as Installs<'d, M::Inner>>::call(child)
            .into_install();
        executor::raw::Install::new(owner, proof)
    }
}

impl<'d, M> executor::raw::Finishes<'d, M> for Projected<M>
where
    M: Projection<'d>,
    M::Inner: Manifold<'d>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, M>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, M> {
        let (mut owner, install, proof) = finish.into_parts();
        let child =
            executor::raw::Finish::new(Projection::inner_mut(owner.as_mut()), install, proof);
        let (install, proof) = <<M::Inner as Manifold<'d>>::Finish as executor::raw::Finishes<
            'd,
            M::Inner,
        >>::call(child)
        .into_finish();
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<'d, M> Installs<'d, M> for Plain
where
    M: Install<'d>,
{
    fn call<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, M>,
    ) -> executor::raw::Install<'app, 'owner, 'd, M> {
        let (mut owner, proof) = install.into_parts();
        Install::install(owner.as_mut());
        executor::raw::Install::new(owner, proof)
    }
}

/// # Safety
/// The owner remains pinned through `shutdown` and `finish`; deferred events
/// and borrowed lifecycle contexts may not escape their dispatch call.
pub unsafe trait Manifold<'d>: Sized {
    /// Canonical route identity installed for this manifold.
    type Route: route::App;

    /// Unique numeric route derived from the canonical route identity.
    const ID: route::Id = <Self::Route as route::App>::ID;

    /// Exact number of coordinator probes needed to observe one idle sweep.
    /// The count remains fixed from installation through finish.
    const COORDS: usize;

    /// Statically selected completion capability.
    type Dispatch: event::Set<'d, Self, Self::Route>;
    /// Statically selected installation capability.
    type Install: Installs<'d, Self>;
    /// Statically selected finalization capability.
    type Finish: executor::raw::Finishes<'d, Self>;
    /// Statically selected activation capability.
    type Activate: Wakes<'d, Self, Self::Route>;
    /// Statically selected liveness-query capability.
    type Probe: Probe<'d, Self>;
    /// Driver authority required immediately before parking.
    type PrePark;
    /// Driver authority required while shutting down.
    type Shutdown;

    /// # Safety
    /// The enclosing exact dispatcher must have installed this pinned owner.
    unsafe fn pre_park<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root;

    fn shutdown<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root;
}

const _: () = {
    assert!(
        std::mem::size_of::<Context<'_, '_, '_, Plain>>()
            == std::mem::size_of::<driver::Context<'_, '_>>()
    );
    assert!(
        std::mem::align_of::<Context<'_, '_, '_, Plain>>()
            == std::mem::align_of::<driver::Context<'_, '_>>()
    );
    assert!(
        std::mem::size_of::<Context<'_, '_, '_, Retained>>()
            == std::mem::size_of::<retained::Context<'_, '_, '_>>()
    );
    assert!(
        std::mem::align_of::<Context<'_, '_, '_, Retained>>()
            == std::mem::align_of::<retained::Context<'_, '_, '_>>()
    );
};

/// Exact top-level installation authority for one coordination step.
#[repr(transparent)]
pub struct Installed<'step, 'd: 'step, T: ?Sized> {
    owner: pin::Pin<&'step mut T>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'step, 'd: 'step, T: ?Sized> Installed<'step, 'd, T> {
    pub(in crate::app::dispatch::raw) fn new(owner: pin::Pin<&'step mut T>) -> Self {
        Self {
            owner,
            driver: marker::PhantomData,
        }
    }

    pub fn as_ref(&self) -> pin::Pin<&T> {
        self.owner.as_ref()
    }

    pub fn as_mut(&mut self) -> pin::Pin<&mut T> {
        self.owner.as_mut()
    }

    pub fn project<'a, U: ?Sized>(
        &'a mut self,
        project: impl FnOnce(pin::Pin<&'a mut T>) -> pin::Pin<&'a mut U>,
    ) -> Loan<'a, 'd, U> {
        self.project_with(|owner| (project(owner), ())).0
    }

    /// Projects one installed field while retaining a disjoint companion.
    pub fn project_with<'a, U: ?Sized, V>(
        &'a mut self,
        project: impl FnOnce(pin::Pin<&'a mut T>) -> (pin::Pin<&'a mut U>, V),
    ) -> (Loan<'a, 'd, U>, V) {
        let (owner, value) = project(self.owner.as_mut());
        (Loan::new(owner), value)
    }
}

const _: () = {
    assert!(mem::size_of::<Installed<'_, '_, ()>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Installed<'_, '_, ()>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Installed<'_, '_, ()>>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Option<Installed<'_, '_, ()>>>() == mem::align_of::<usize>());
};

/// Exclusive subowner access bounded by one coordination step.
///
/// ```compile_fail
/// use dope_manifold::app::dispatch::raw::Loan;
/// use std::pin::Pin;
///
/// fn escape<T: ?Sized>(loan: Loan<'_, '_, T>) -> Pin<&'static mut T> {
///     loan.into_pin()
/// }
/// ```
///
/// ```compile_fail
/// use dope_manifold::app::dispatch::raw::Loan;
///
/// fn substitute<'step, 'long: 'step, 'short: 'step, T>(
///     loan: Loan<'step, 'long, T>,
/// ) -> Loan<'step, 'short, T>
/// where
///     'long: 'short,
/// {
///     loan
/// }
/// ```
///
/// ```compile_fail
/// use dope_manifold::app::dispatch::raw::{Installed, Loan};
///
/// fn upgrade<'step, 'd: 'step, T>(
///     loan: Loan<'step, 'd, T>,
/// ) -> Installed<'step, 'd, T> {
///     loan.into()
/// }
/// ```
#[repr(transparent)]
pub struct Loan<'step, 'd: 'step, T: ?Sized> {
    owner: pin::Pin<&'step mut T>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'step, 'd: 'step, T: ?Sized> Loan<'step, 'd, T> {
    pub(in crate::app::dispatch::raw) fn new(owner: pin::Pin<&'step mut T>) -> Self {
        Self {
            owner,
            driver: marker::PhantomData,
        }
    }

    pub fn into_pin(self) -> pin::Pin<&'step mut T> {
        self.owner
    }
}

const _: () = {
    assert!(mem::size_of::<Loan<'static, 'static, ()>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Loan<'static, 'static, ()>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Loan<'static, 'static, ()>>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Option<Loan<'static, 'static, ()>>>() == mem::align_of::<usize>());
};

/// A coordination view rooted in one exactly installed manifold.
///
/// # Safety
/// The view must not move or release storage backed by the installed owner.
pub unsafe trait Control<'d>: Manifold<'d> {
    type View<'step>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step;

    /// # Safety
    /// `owner` is the exact root projection installed for all of `'d`.
    unsafe fn root<'step>(owner: pin::Pin<&'step mut Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        Self::view(Installed::new(owner))
    }
}
