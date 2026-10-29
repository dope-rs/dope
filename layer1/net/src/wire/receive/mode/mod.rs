//! Type-level receive-ingress capabilities.

use std::marker;

use dope_core::driver::{
    receive::{credit, credit::error::Arm},
    route::Identity,
    schedule::ready,
};

pub mod access;
mod direct;
mod kind;
/// A zero-sized binding selected when receive credit is not retained.
pub use direct::Direct;
pub(crate) use kind::Kind;

/// A compile-time capability flag.
pub trait Flag: Kind {
    /// Route binding selected by this capability.
    type Binding<'d, ID: Identity>: Copy;

    /// Union of this capability and another capability.
    type Or<Rhs: Flag>: Flag;

    /// This capability with completion parking added, without necessarily
    /// adding retained receive credit storage.
    type WithParking: Flag;
}

/// Wire cursor credit is either absent or retained; parking is not a credit.
pub trait Credit: Flag {
    /// Receive-credit proof retained after the callback returns.
    type Guard<'d>: Unpin + 'd;
}

/// Ingress capability backed by one stored receive-credit binding.
pub(crate) trait Stored: Flag {
    /// Arms the exact route-branded binding for its current readiness owner.
    fn arm<'d, ID: Identity>(
        binding: Self::Binding<'d, ID>,
        ready: ready::Target<'d>,
    ) -> Result<credit::Pending<'d, ID>, Arm>;
}

/// Capability is absent. This marker has no values or runtime representation.
pub enum Disabled {}

/// Receive completions may be parked, but no retained receive credit can be
/// minted. This is the exact capability needed by application-owned ingress
/// gates over wires whose receive reservation cannot backpressure.
pub enum Parking {}

/// Capability is present. This marker has no values or runtime representation.
pub enum Enabled {}

/// Receive credit backed by one exact recoverable reservation policy.
pub struct Waiting<R>(marker::PhantomData<fn() -> R>);

impl Kind for Disabled {}
impl Kind for Parking {}
impl Kind for Enabled {}
impl<R> Kind for Waiting<R> {}

impl Credit for Disabled {
    type Guard<'d> = ();
}

impl Credit for Enabled {
    type Guard<'d> = credit::Return<'d>;
}

impl Stored for Enabled {
    fn arm<'d, ID: Identity>(
        binding: Self::Binding<'d, ID>,
        ready: ready::Target<'d>,
    ) -> Result<credit::Pending<'d, ID>, Arm> {
        let (control, target) = binding.into_parts();
        control.arm(target, ready)
    }
}

impl<R> Stored for Waiting<R> {
    fn arm<'d, ID: Identity>(
        binding: Self::Binding<'d, ID>,
        ready: ready::Target<'d>,
    ) -> Result<credit::Pending<'d, ID>, Arm> {
        let (control, target) = binding.into_parts();
        control.arm(target, ready)
    }
}

impl Flag for Disabled {
    type Binding<'d, ID: Identity> = Direct<'d, ID>;
    type Or<Rhs: Flag> = Rhs;
    type WithParking = Parking;
}

impl Flag for Parking {
    type Binding<'d, ID: Identity> = Direct<'d, ID>;
    type Or<Rhs: Flag> = Rhs::WithParking;
    type WithParking = Parking;
}

impl Flag for Enabled {
    type Binding<'d, ID: Identity> = credit::Binding<'d, ID>;
    type Or<Rhs: Flag> = Enabled;
    type WithParking = Enabled;
}

impl<R> Flag for Waiting<R> {
    type Binding<'d, ID: Identity> = credit::Binding<'d, ID>;
    type Or<Rhs: Flag> = Self;
    type WithParking = Self;
}
