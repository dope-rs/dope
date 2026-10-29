use std::{mem, pin};

use o3::{
    alloc,
    base::cell::region,
    buffer::{
        hold::limits,
        resident::{Budget, Reuse},
    },
};

use crate::{
    BranchError, driver,
    driver::{
        life,
        retained::flight,
        route::{App, lease, queues},
        schedule::{clock, clock::timer},
        settings,
        settings::{claim, errors},
    },
};

mod cleanup;
mod failure;
mod mapping;
pub mod path;
mod release;
mod removal;
pub mod resident;
mod scope;

pub use cleanup::Cleanup;
pub use failure::Failure;
pub use mapping::Mapping;
pub use release::Release;
pub use removal::Removal;
pub use scope::Scope;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Address(*mut core::ffi::c_void);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Bytes(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Count(usize);

impl Address {
    pub(crate) const fn new(address: *mut core::ffi::c_void) -> Self {
        Self(address)
    }
}

impl Bytes {
    pub(crate) const fn new(bytes: usize) -> Self {
        Self(bytes)
    }
}

impl Count {
    pub(crate) const fn new(count: usize) -> Self {
        Self(count)
    }
}

/// Resident bytes requested for one driver-lifetime accounting domain.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Resident(usize);

/// Failure to issue one resident domain from a bounded allocator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exhausted {
    Domains { capacity: usize },
    Retained { index: usize, cause: Reuse },
}

const _: () = assert!(size_of::<Exhausted>() == size_of::<usize>() * 4);

impl Resident {
    pub const fn bytes(bytes: usize) -> Self {
        Self(bytes)
    }

    pub const fn from_limit(limit: limits::Bound) -> Self {
        Self(limit.bytes())
    }

    const fn get(self) -> usize {
        self.0
    }
}

/// Projects one exact leaf from a generative storage build output.
pub trait Path<Root, Leaf>: Copy
where
    Root: Plan,
    Leaf: Plan,
{
    fn get<'life, 'p, 'd>(self, root: &'life Root::Output<'p, 'd>) -> &'life Leaf::Output<'p, 'd>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        Root: 'd,
        Leaf: 'd;
}

/// Restricted construction context for driver-domain storage.
///
/// Exposes stable identities and route reservation without operation submission.
/// A build cannot publish into its output before the runtime pins that output.
///
#[repr(transparent)]
pub struct Context<'a, 'd>(pub(in crate::driver) &'a mut life::Local<'d>);

impl<'a, 'd> Context<'a, 'd> {
    pub(super) fn new(local: &'a mut life::Local<'d>) -> Self {
        Self(local)
    }

    pub fn driver(&self) -> driver::Reference<'d> {
        self.0.driver
    }

    /// Exact receive-pool geometry of this driver generation.
    pub fn receive(&self) -> settings::Receive {
        self.0.driver.receive().layout()
    }

    pub fn region(&self) -> &region::Token<region::Identity<'d>> {
        &self.0.region.0
    }

    pub fn state(&self) -> &region::Token<region::Identity<'d, 1>> {
        &self.0.region.1
    }

    pub fn timer(&self) -> timer::Timer<'d> {
        self.0.driver.timer()
    }

    pub fn clock(&self) -> clock::Clock<'_, 'd> {
        self.0.driver.scheduler().clock()
    }

    /// Copies the resident-domain authority for this exact driver generation.
    pub fn allocator(&self) -> resident::Alloc<'d> {
        self.0.resident
    }

    /// Claims a lifetime-branded resident accounting domain for storage owned
    /// by the build output.
    pub fn budget(&self, cap: Resident) -> Result<Budget<'d>, Exhausted> {
        self.0.resident.try_budget(cap.get())
    }

    /// Lends one affine construction step while retaining this outer authority.
    pub fn reborrow(&mut self) -> Context<'_, 'd> {
        Context(&mut *self.0)
    }

    /// Stages fixed submission groups without changing the live flight arena.
    pub fn try_flights<Tags, const GROUPS: usize>(
        self,
        need: flight::Need<Tags, GROUPS>,
    ) -> Result<flight::Prepared<'a, 'd, Tags, GROUPS>, alloc::Error> {
        let Self(local) = self;
        local.flights.try_prepare(need, local.driver)
    }

    pub fn try_reserve<B: App>(self) -> Result<lease::Claim<'d, B>, lease::Error> {
        let Self(local) = self;
        lease::Claim::try_new(local)
    }

    pub(in crate::driver) fn release(&mut self, retired: queues::Retired<'d>) {
        lease::Registry::release(self.0, retired);
    }
}

/// One generative storage output family.
///
/// A plan may borrow from its enclosing owner. Its output is available only
/// for driver generations which the consumed plan itself outlives.
pub trait Plan {
    type Output<'p, 'd>: 'p
    where
        Self: 'd,
        'd: 'p;
}

/// Pinned storage retaining its plan, backing loan, and driver generation.
#[repr(transparent)]
pub struct Loan<'a, 'p: 'a, 'd: 'p, S: Plan + 'd> {
    value: pin::Pin<&'a S::Output<'p, 'd>>,
}

impl<'a, 'p: 'a, 'd: 'p, S: Plan + 'd> Loan<'a, 'p, 'd, S> {
    pub(in crate::driver) fn new(value: pin::Pin<&'a S::Output<'p, 'd>>) -> Self {
        Self { value }
    }

    /// Preserves the source borrow when this temporary view is consumed.
    ///
    ///
    pub fn pinned(&self) -> pin::Pin<&'a S::Output<'p, 'd>> {
        self.value
    }
}

const _: () = {
    assert!(mem::size_of::<Loan<'static, 'static, 'static, ()>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Loan<'static, 'static, 'static, ()>>() == mem::align_of::<usize>());
};

/// Fallibly constructs driver-domain storage through the non-submitting [`Context`].
pub trait Build: Plan {
    type Error;

    type Store<'d>
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd;

    fn try_build<'p, 'd>(
        self,
        store: &'p mut Self::Store<'d>,
        ctx: Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p;
}

/// Constructs driver-domain storage that owns all of its backing.
pub trait Create: Plan {
    type Error;

    fn try_build<'p, 'd>(self, ctx: Context<'_, 'd>) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p;
}

/// Builds storage without contributing driver-resource claims.
pub trait Claimless: Plan {}

/// Constructs storage without introducing a recoverable failure path.
pub trait Direct: Plan {
    fn build<'p, 'd>(self) -> Self::Output<'p, 'd>
    where
        Self: 'd,
        'd: 'p;
}

#[repr(transparent)]
pub struct Value<T>(T);

impl<T> Value<T> {
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> Plan for Value<T> {
    type Output<'p, 'd>
        = T
    where
        'd: 'p,
        Self: 'd;
}

impl<T> Claimless for Value<T> {}

impl<T> Direct for Value<T> {
    fn build<'p, 'd>(self) -> Self::Output<'p, 'd>
    where
        'd: 'p,
        Self: 'd,
    {
        self.0
    }
}

impl Plan for () {
    type Output<'p, 'd>
        = ()
    where
        'd: 'p,
        Self: 'd;
}

impl Claimless for () {}

impl Direct for () {
    fn build<'p, 'd>(self) -> Self::Output<'p, 'd>
    where
        'd: 'p,
        Self: 'd,
    {
        self
    }
}

impl<A: Plan, B: Plan> Plan for (A, B) {
    type Output<'p, 'd>
        = (A::Output<'p, 'd>, B::Output<'p, 'd>)
    where
        'd: 'p,
        Self: 'd;
}

impl<A: Build, B: Build> Build for (A, B) {
    type Error = BranchError<A::Error, B::Error>;
    type Store<'d>
        = (A::Store<'d>, B::Store<'d>)
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        let first = self.0.try_store().map_err(BranchError::First)?;
        let second = self.1.try_store().map_err(BranchError::Second)?;
        Ok((first, second))
    }

    fn try_build<'p, 'd>(
        self,
        store: &'p mut Self::Store<'d>,
        mut ctx: Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        'd: 'p,
        Self: 'd,
    {
        let (first, second) = self;
        let first = first
            .try_build(&mut store.0, ctx.reborrow())
            .map_err(BranchError::First)?;
        let second = second
            .try_build(&mut store.1, ctx)
            .map_err(BranchError::Second)?;
        Ok((first, second))
    }
}

impl<A: Claimless, B: Claimless> Claimless for (A, B) {}

impl<A: Direct, B: Direct> Direct for (A, B) {
    fn build<'p, 'd>(self) -> Self::Output<'p, 'd>
    where
        'd: 'p,
        Self: 'd,
    {
        let first = Direct::build(self.0);
        let second = Direct::build(self.1);
        (first, second)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Context<'static, 'static>>() == std::mem::size_of::<usize>());
    assert!(std::mem::align_of::<Context<'static, 'static>>() == std::mem::align_of::<usize>());
    assert!(std::mem::size_of::<Value<usize>>() == std::mem::size_of::<usize>());
    assert!(std::mem::align_of::<Value<usize>>() == std::mem::align_of::<usize>());
    assert!(std::mem::needs_drop::<Value<String>>() == std::mem::needs_drop::<String>());
};

/// Builds one storage plan while carrying an adjacent owned value.
#[repr(transparent)]
pub struct With<F, T>((F, T));

impl<F, T> With<F, T> {
    pub const fn new(plan: F, value: T) -> Self {
        Self((plan, value))
    }
}

impl<F, T> settings::Resource for With<F, T>
where
    F: settings::Resource,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.0.0.include_in(needs)
    }
}

impl<F, T> Plan for With<F, T>
where
    F: Plan,
{
    type Output<'p, 'd>
        = (F::Output<'p, 'd>, T)
    where
        'd: 'p,
        Self: 'd;
}

impl<F, T> Build for With<F, T>
where
    F: Build,
{
    type Error = F::Error;
    type Store<'d>
        = F::Store<'d>
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        self.0.0.try_store()
    }

    fn try_build<'p, 'd>(
        self,
        store: &'p mut Self::Store<'d>,
        ctx: Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        'd: 'p,
        Self: 'd,
    {
        let (plan, value) = self.0;
        Ok((plan.try_build(store, ctx)?, value))
    }
}

impl<F: Direct, T> Direct for With<F, T> {
    fn build<'p, 'd>(self) -> Self::Output<'p, 'd>
    where
        'd: 'p,
        Self: 'd,
    {
        let (plan, value) = self.0;
        (plan.build(), value)
    }
}

impl<F: Claimless, T> Claimless for With<F, T> {}

const _: () = {
    assert!(mem::size_of::<With<usize, u8>>() == mem::size_of::<(usize, u8)>());
    assert!(mem::align_of::<With<usize, u8>>() == mem::align_of::<(usize, u8)>());
    assert!(mem::needs_drop::<With<String, ()>>() == mem::needs_drop::<String>());
};
