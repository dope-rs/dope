use std::marker::PhantomData;

use dope_core::driver::{self, settings::Domain, storage};
use dope_net::{link::pool::recv::input, wire};
use o3::buffer::resident;
use wire::{domain, receive};

use crate::app::creation;

mod failure;
mod layout;
pub mod limits;

/// Application-ingress disposition for one complete receive transition.
#[must_use = "the ingress outcome must be applied to the connection"]
pub enum Outcome {
    Continue,
    CloseAfter,
    Failed(Failure),
}

/// Directions ended by the receive transition, after its plaintext is delivered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum End {
    /// Input ended without this transition ending output.
    /// Output may already have been closed locally.
    Read,
    /// The wire ended both directions; terminal output may still be draining.
    Both,
}

pub mod ingress;
mod policy;

pub use failure::Failure;
pub(crate) use policy::Policy;

pub struct Borrowed;
/// Callback-borrowed delivery whose connection ingress is suspended while
/// listener egress remains live. The mode retains no application input and
/// adds no connection-local state.
pub struct Coupled;
/// Callback-borrowed delivery with an application gate and egress fence.
/// One deferred lane preserves completions racing the gate in their original
/// generation; live egress suspends ingress without application shadow state.
pub struct Gated;
/// A cursor owned by the receive transaction but borrowed by the application
/// for exactly one callback. Exact ranges may be retained into the driver
/// domain; the wire cursor itself cannot escape the callback.
/// `GATED` enables application and egress gates with ordinary send callbacks.
pub struct Scoped<const GATED: bool = false>;
/// Callback-scoped cursor retention joined to a receive gate and egress fence.
/// Exact retained ranges charge the driver-domain budget while the cursor
/// borrow cannot escape, preserving a causal zero-copy handoff.
pub struct GatedScoped;
pub struct Owned<R = ()>(PhantomData<fn() -> R>);

trait Batch: Plain + Policy {}
trait Plain {}

impl Batch for Borrowed {}
impl Batch for Coupled {}
impl Batch for Gated {}
impl Plain for Borrowed {}
impl Plain for Coupled {}
impl Plain for Gated {}
impl<R> Plain for Owned<R> {}

/// Driver resident-domain demand and its exact receive authority.
pub trait Resources: Sized {
    const RESIDENT_DOMAINS: Domain;

    /// Modes which cannot mint resident owners carry `()`.
    type Authority<'d>;

    fn provision<'d>(
        cap: domain::Resident,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Self::Authority<'d>, creation::Error>;
}

impl<I: Plain> Resources for I {
    const RESIDENT_DOMAINS: Domain = Domain::ZERO;
    type Authority<'d> = ();

    fn provision<'d>(
        _: domain::Resident,
        _: &mut driver::Context<'_, 'd>,
    ) -> Result<(), creation::Error> {
        Ok(())
    }
}

impl<const GATED: bool> Resources for Scoped<GATED> {
    const RESIDENT_DOMAINS: Domain = Domain::ONE;
    type Authority<'d> = resident::Budget<'d>;

    fn provision<'d>(
        cap: domain::Resident,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<resident::Budget<'d>, creation::Error> {
        driver
            .storage
            .budget(storage::Resident::bytes(cap.into_raw()))
            .map_err(creation::Error::Resident)
    }
}

impl Resources for GatedScoped {
    const RESIDENT_DOMAINS: Domain = Domain::ONE;
    type Authority<'d> = resident::Budget<'d>;

    fn provision<'d>(
        cap: domain::Resident,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<resident::Budget<'d>, creation::Error> {
        Scoped::<false>::provision(cap, driver)
    }
}

pub trait Mode<W: wire::Spec>: input::Mode<W> + Policy + Resources + Sized {}

impl Policy for Borrowed {}
impl Policy for Coupled {
    const EGRESS_COUPLED: bool = true;
}
impl Policy for Gated {
    const EGRESS_COUPLED: bool = true;
    const APPLICATION_GATED: bool = true;
}
impl<const GATED: bool> Policy for Scoped<GATED> {
    const EGRESS_COUPLED: bool = GATED;
    const APPLICATION_GATED: bool = GATED;
}
impl Policy for GatedScoped {
    const EGRESS_COUPLED: bool = true;
    const APPLICATION_GATED: bool = true;
}
impl<R> Policy for Owned<R> {}

impl<W> input::Mode<W> for Borrowed
where
    W: wire::Spec,
    input::Borrowed: input::Kind<W>,
{
    type Kind = input::Borrowed;
    type Receipt = ();
    type Ingress = <input::Borrowed as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for Coupled
where
    W: wire::Spec,
    input::Coupled: input::Kind<W>,
{
    type Kind = input::Coupled;
    type Receipt = ();
    type Ingress = <input::Coupled as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for Gated
where
    W: wire::Spec,
    input::Coupled: input::Kind<W>,
{
    type Kind = input::Coupled;
    type Receipt = ();
    type Ingress = <input::Coupled as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W, const GATED: bool> input::Mode<W> for Scoped<GATED>
where
    W: wire::Spec,
    input::Scoped: input::Kind<W>,
{
    type Kind = input::Scoped;
    type Receipt = ();
    type Ingress = <input::Scoped as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for GatedScoped
where
    W: wire::Spec,
    input::Scoped: input::Kind<W>,
{
    type Kind = input::Scoped;
    type Receipt = ();
    type Ingress = <input::Scoped as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W, R: Default> input::Mode<W> for Owned<R>
where
    W: input::Execution,
{
    type Kind = input::Retained;
    type Receipt = R;
    type Ingress = W::Retained;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W, I> Mode<W> for I
where
    W: wire::Spec,
    I: Batch + input::Mode<W>,
{
}

impl<W, const GATED: bool> Mode<W> for Scoped<GATED>
where
    W: wire::Spec,
    Scoped<GATED>: input::Mode<W>,
    <Scoped<GATED> as input::Mode<W>>::Kind: input::Replay<W>,
{
}

impl<W> Mode<W> for GatedScoped
where
    W: wire::Spec,
    GatedScoped: input::Mode<W>,
    <GatedScoped as input::Mode<W>>::Kind: input::Replay<W>,
{
}

impl<W, R: Default> Mode<W> for Owned<R>
where
    W: wire::Spec,
    Owned<R>: input::Mode<W>,
{
}
