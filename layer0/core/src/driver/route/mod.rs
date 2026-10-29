mod epoch;
pub mod indices;
use core::{hash, marker, mem, num};

pub use epoch::Epoch;
use indices::SlotIndex;
pub use token::Token;
pub(crate) use token::{Brand, OutboundKey};

use crate::driver::route;

pub mod lease;
pub(in crate::driver) mod queues;
pub mod raw;
pub mod table;
mod token;

pub(crate) const FRAMEWORK: u8 = 255;
pub(crate) const CAPACITY: usize = u8::MAX as usize + 1;

pub(crate) const SHIFT: u32 = 56;
pub(crate) const KIND_SHIFT: u32 = 48;
pub const SLOT_BITS: u32 = 24;
pub const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
pub(crate) const AUX_SHIFT: u32 = SLOT_BITS;
pub(crate) const AUX_BITS: u32 = KIND_SHIFT - AUX_SHIFT;
pub(crate) const AUX_VALUE_MASK: u32 = (1 << AUX_BITS) - 1;
pub(crate) const AUX_MASK: u64 = (AUX_VALUE_MASK as u64) << AUX_SHIFT;

/// A validated application route identity. The framework route cannot be
/// constructed as an `Id`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Id(u8);

/// The route namespace observed on a token or event.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct Route(u8);

impl Id {
    pub const fn fixed<const ID: u8>() -> Self {
        const { assert!(ID != FRAMEWORK, "framework route is reserved") };
        Self(ID)
    }

    /// Exact numeric identity, for hash domains and retained diagnostics.
    pub const fn raw(self) -> u8 {
        self.0
    }

    pub(crate) const fn into_route(self) -> Route {
        Route(self.0)
    }
}

impl Route {
    pub(crate) const FRAMEWORK: Self = Self(FRAMEWORK);

    pub(crate) const fn app(self) -> Option<Id> {
        if self.is_framework() {
            None
        } else {
            Some(Id(self.0))
        }
    }

    pub(crate) const fn from_bits(raw: u8) -> Self {
        Self(raw)
    }

    pub const fn is_framework(self) -> bool {
        self.0 == FRAMEWORK
    }

    pub(crate) const fn raw(self) -> u8 {
        self.0
    }
}

impl From<Id> for Route {
    fn from(id: Id) -> Self {
        Self(id.raw())
    }
}

impl TryFrom<Route> for Id {
    type Error = Route;

    fn try_from(route: Route) -> Result<Self, Self::Error> {
        route.app().ok_or(route)
    }
}

/// A route tag carrying its operation class as a type.
///
///
#[derive(Clone, Copy, Debug)]
pub struct KeyTag<const ROUTE: u8, C: kind::Class = kind::Any>(marker::PhantomData<C>);

/// A framework-owned route tag for one registered operation class.
#[derive(Clone, Copy)]
pub struct FrameworkTag<C: kind::Class>(marker::PhantomData<C>);

/// A route tag carries its identity through every invariant branded target.
pub trait Tag: Copy {
    const ROUTE: Route;
    type Class: kind::Class;
}

/// Proves that a target tag may be materialized as one operation class.
pub trait Select<C: kind::Class>: Tag {}

/// A route tag proven to belong to the application namespace.
pub trait App: Tag + kind::Sealed {
    const ID: Id;
}

/// One application route identity usable in every operation class.
///
/// It carries [`Select`]'s class selection instead of a `const ID: u8` brand,
/// so it adds no authority the class marker withheld.
pub trait Identity:
    App<Class = kind::Any>
    + Eq
    + hash::Hash
    + Materialize<kind::Accept>
    + Materialize<kind::Recv>
    + Materialize<kind::Send>
    + Materialize<kind::Socket>
    + Materialize<kind::Connect>
    + Materialize<kind::Shutdown>
    + Materialize<kind::Open>
    + Materialize<kind::OneShot>
    + Materialize<kind::TaskQueue>
    + Materialize<kind::Tuning>
    + Materialize<kind::Write>
    + Materialize<kind::Sync>
{
}

/// One route identity materialized as a specific operation class.
///
/// The class a route is used in is part of the tag's type, so naming it is a
/// type-level projection this trait carries for an unspecified class.
pub trait Materialize<C: kind::Class>: Tag<Class = kind::Any> {
    type With: App<Class = C> + Select<C>;
}

impl<const ROUTE: u8> Identity for KeyTag<ROUTE, kind::Any> {}

impl<const ROUTE: u8, C: kind::Class> Materialize<C> for KeyTag<ROUTE, kind::Any>
where
    KeyTag<ROUTE, C>: App<Class = C> + Select<C>,
{
    type With = KeyTag<ROUTE, C>;
}

/// A tag carries only its route and class as types, so every value of one tag
/// type is the same identity and hashes by that route.
impl<const ROUTE: u8, C: kind::Class> PartialEq for KeyTag<ROUTE, C> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<const ROUTE: u8, C: kind::Class> Eq for KeyTag<ROUTE, C> {}

impl<const ROUTE: u8, C: kind::Class> hash::Hash for KeyTag<ROUTE, C> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        state.write_u8(ROUTE);
    }
}

impl<const ROUTE: u8, C: kind::Class> Tag for KeyTag<ROUTE, C> {
    const ROUTE: Route = Id::fixed::<ROUTE>().into_route();
    type Class = C;
}

impl<const ROUTE: u8, C: kind::Class> App for KeyTag<ROUTE, C> {
    const ID: Id = Id::fixed::<ROUTE>();
}

impl<const ROUTE: u8, C: kind::Class> kind::Sealed for KeyTag<ROUTE, C> {}

impl<C: kind::Class> Tag for FrameworkTag<C> {
    const ROUTE: Route = Route::FRAMEWORK;
    type Class = C;
}

impl<T: Tag<Class = kind::Any>, C: kind::Class> Select<C> for T {}
/// One operation identity branded by its driver lifetime and route.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Operation<'d, T: Tag, C: kind::Class = <T as Tag>::Class> {
    token: Token,
    driver: Brand<'d, T>,
    class: marker::PhantomData<C>,
}

impl<'d, T: Tag, C: kind::Class> Operation<'d, T, C> {
    pub(super) const fn new(token: Token, driver: Brand<'d, T>) -> Self {
        Self {
            token,
            driver,
            class: marker::PhantomData,
        }
    }

    pub fn matches(self, target: Erased<'d, C>) -> bool {
        self.token == target.token
    }

    pub const fn parts(self) -> table::Parts<T> {
        table::Parts::new(self.token.slot(), self.token.epoch())
    }

    pub const fn erase_route(self) -> Erased<'d, C> {
        Erased::new(self.token, self.driver.driver).bind(kind::Witness(self.class))
    }

    pub(crate) const fn into_token(self) -> Token {
        self.token
    }
}

impl<T: Tag, C: kind::Class> PartialEq for Operation<'_, T, C> {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl<T: Tag, C: kind::Class> Eq for Operation<'_, T, C> {}

impl<T: Tag, C: kind::Class> hash::Hash for Operation<'_, T, C> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.token.hash(state);
    }
}

/// Operation identity after deliberately erasing only its route tag.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Erased<'d, C: kind::Class> {
    pub(in crate::driver) token: Token,
    pub(in crate::driver) driver: super::Invariant<'d>,
    pub(in crate::driver) class: marker::PhantomData<C>,
}

impl<'d> Erased<'d, kind::Any> {
    pub(in crate::driver) const fn new(token: Token, driver: super::Invariant<'d>) -> Self {
        Self {
            token,
            driver,
            class: marker::PhantomData,
        }
    }

    pub(crate) const fn bind<C: kind::Class>(self, proof: kind::Witness<C>) -> Erased<'d, C> {
        let kind::Witness(class) = proof;
        Erased {
            token: self.token,
            driver: self.driver,
            class,
        }
    }
}

impl<'d, C: kind::Class> Erased<'d, C> {
    pub fn matches(self, target: Self) -> bool {
        self.token == target.token
    }

    pub const fn route(self) -> Route {
        self.token.route()
    }

    pub const fn slot(self) -> SlotIndex {
        self.token.slot()
    }

    pub const fn epoch(self) -> Epoch {
        self.token.epoch()
    }

    pub(crate) const fn into_token(self) -> Token {
        self.token
    }
}

/// A class-typed operation that belongs to another route.
pub struct Mismatch<'d, T: Tag, C: kind::Class> {
    target: Erased<'d, C>,
    tag: marker::PhantomData<T>,
}

impl<'d, T: Tag, C: kind::Class> Mismatch<'d, T, C> {
    const fn new(target: Erased<'d, C>) -> Self {
        Self {
            target,
            tag: marker::PhantomData,
        }
    }

    pub fn into_target(self) -> Erased<'d, C> {
        self.target
    }
}

pub mod kind;

pub const SHUTDOWN: Control = Control::new(<kind::Shutdown as kind::Class>::CODE, SlotIndex::ZERO);

const _: () = {
    assert!(mem::size_of::<Erased<'static, kind::Send>>() == mem::size_of::<Token>());
    assert!(mem::align_of::<Erased<'static, kind::Send>>() == mem::align_of::<Token>());
    assert!(mem::size_of::<Mismatch<'static, KeyTag<1>, kind::Send>>() == mem::size_of::<Token>());
};
const _: () = assert!(mem::size_of::<Id>() == mem::size_of::<u8>());
const _: () = assert!(mem::size_of::<Route>() == mem::size_of::<u8>());
const _: () = {
    assert!(mem::size_of::<Operation<'static, KeyTag<1>>>() == mem::size_of::<Token>());
    assert!(mem::align_of::<Operation<'static, KeyTag<1>>>() == mem::align_of::<Token>());
};

/// One framework control decoded without manufacturing a keyed identity.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Control(num::NonZero<u64>);

impl From<Control> for num::NonZero<u64> {
    fn from(control: Control) -> Self {
        control.0
    }
}

impl Control {
    pub(crate) const fn new(kind: kind::Code, slot: indices::SlotIndex) -> Self {
        let raw = ((route::FRAMEWORK as u64) << route::SHIFT)
            | ((kind.raw() as u64) << route::KIND_SHIFT)
            | slot.raw() as u64;
        // SAFETY: the nonzero framework route occupies the high byte.
        Self(unsafe { num::NonZero::<u64>::new_unchecked(raw) })
    }

    pub const fn slot(self) -> indices::SlotIndex {
        indices::SlotIndex::from_field(self.0.get())
    }
}

impl TryFrom<u64> for Control {
    type Error = u64;

    fn try_from(raw: u64) -> Result<Self, Self::Error> {
        if (raw >> route::SHIFT) as u8 != route::FRAMEWORK {
            return Err(raw);
        }
        // SAFETY: a framework-tagged high byte makes the complete word nonzero.
        Ok(Self(unsafe { num::NonZero::<u64>::new_unchecked(raw) }))
    }
}

const _: () = {
    assert!(mem::size_of::<Token>() == 2 * mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Token>>() == mem::size_of::<Token>());
    assert!(mem::size_of::<Control>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Control>>() == mem::size_of::<Control>());
};

/// Zero-sized constructor branding table identities with one driver.
#[derive(Clone, Copy)]
pub struct Space<'d, Tag: route::Tag> {
    pub(in crate::driver) driver: route::Brand<'d, Tag>,
}

impl<'d, Tag: route::Tag> Space<'d, Tag> {
    pub const fn bind(
        self,
        slot: indices::SlotIndex,
        epoch: route::Epoch,
    ) -> route::Target<'d, Tag> {
        route::Target::from_parts(table::Parts::new(slot, epoch), self.driver)
    }

    pub fn parse<C: kind::Class>(
        self,
        target: route::Erased<'d, C>,
    ) -> Result<route::Operation<'d, Tag, C>, route::Mismatch<'d, Tag, C>>
    where
        Tag: route::Select<C>,
    {
        if target.route() != Tag::ROUTE {
            return Err(route::Mismatch::new(target));
        }
        Ok(route::Operation::new(target.into_token(), self.driver))
    }
}

const _: () = assert!(mem::size_of::<Space<'static, route::KeyTag<1>>>() == 0);

/// One generation-checked target branded by its driver lifetime and route.
///
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Target<'d, Tag: route::Tag> {
    parts: table::Parts<Tag>,
    pub(in crate::driver) driver: route::Brand<'d, Tag>,
}

impl<'d, Tag: route::Tag> Target<'d, Tag> {
    pub(in crate::driver::route) const fn from_parts(
        parts: table::Parts<Tag>,
        driver: route::Brand<'d, Tag>,
    ) -> Self {
        Self { parts, driver }
    }

    pub const fn parts(self) -> table::Parts<Tag> {
        self.parts
    }

    pub const fn slot(self) -> indices::SlotIndex {
        self.parts.slot()
    }

    pub const fn epoch(self) -> route::Epoch {
        self.parts.epoch()
    }

    pub const fn bind<T>(self, value: T) -> route::Bound<'d, Tag, T> {
        route::Bound::new(self, value)
    }

    pub const fn operation<C: kind::Class>(self) -> route::Operation<'d, Tag, C>
    where
        Tag: route::Select<C>,
    {
        let Self { parts, driver } = self;
        route::Operation::new(route::Token::encode::<Tag, C>(parts), driver)
    }

    pub const fn dispatch(self) -> route::Operation<'d, Tag> {
        let Self { parts, driver } = self;
        route::Operation::new(route::Token::from_parts(parts), driver)
    }
}

impl<'d, Tag: route::Tag, C: kind::Class> From<&route::Operation<'d, Tag, C>> for Target<'d, Tag> {
    fn from(op: &route::Operation<'d, Tag, C>) -> Self {
        Self {
            parts: op.parts(),
            driver: op.driver,
        }
    }
}

impl<Tag: route::Tag> PartialEq for Target<'_, Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.parts == other.parts
    }
}

impl<Tag: route::Tag> Eq for Target<'_, Tag> {}

impl<Tag: route::Tag> hash::Hash for Target<'_, Tag> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.parts.hash(state);
    }
}

const _: () = {
    assert!(mem::size_of::<Target<'static, route::KeyTag<1>>>() == mem::size_of::<route::Token>());
    assert!(
        mem::align_of::<Target<'static, route::KeyTag<1>>>() == mem::align_of::<route::Token>()
    );
};

/// Affine submission input paired with its exact target.
///
#[must_use = "bound submission input must be submitted or dropped"]
pub struct Bound<'d, Tag: route::Tag, T> {
    target: route::Target<'d, Tag>,
    value: T,
}

impl<'d, Tag: route::Tag, T> Bound<'d, Tag, T> {
    pub(in crate::driver::route) const fn new(target: route::Target<'d, Tag>, value: T) -> Self {
        Self { target, value }
    }

    pub(crate) fn into_parts(self) -> (route::Target<'d, Tag>, T) {
        (self.target, self.value)
    }
}
