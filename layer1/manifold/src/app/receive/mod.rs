use dope_core::driver::{self, settings::Domain, storage};
use dope_net::{
    link::pool::recv::input,
    wire,
    wire::receive::{Chunk, mode::Disabled},
};
use o3::buffer::resident;
use wire::{
    domain,
    receive::{self, batch},
};

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
pub struct Scoped;
/// Callback-scoped cursor retention joined to a receive gate and egress fence.
/// Exact retained ranges charge the driver-domain budget while the cursor
/// borrow cannot escape, preserving a causal zero-copy handoff.
pub struct GatedScoped;
pub struct Owned;

trait Batch: Plain + Policy {}
trait Plain {}

impl Batch for Borrowed {}
impl Batch for Coupled {}
impl Batch for Gated {}
impl Plain for Borrowed {}
impl Plain for Coupled {}
impl Plain for Gated {}
impl Plain for Owned {}

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

impl Resources for Scoped {
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
        Scoped::provision(cap, driver)
    }
}

pub trait Mode<W: wire::Spec>: input::Mode<W> + Policy + Resources + Sized {
    type Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;
}

impl Policy for Borrowed {}
impl Policy for Coupled {
    const EGRESS_COUPLED: bool = true;
}
impl Policy for Gated {
    const EGRESS_COUPLED: bool = true;
    const APPLICATION_GATED: bool = true;
}
impl Policy for Scoped {}
impl Policy for GatedScoped {
    const EGRESS_COUPLED: bool = true;
    const APPLICATION_GATED: bool = true;
}
impl Policy for Owned {}

impl<W> input::Mode<W> for Borrowed
where
    W: wire::Spec,
    input::Borrowed: input::Kind<W>,
{
    type Kind = input::Borrowed;
    type Ingress = <input::Borrowed as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for Coupled
where
    W: wire::Spec,
    input::Coupled: input::Kind<W>,
{
    type Kind = input::Coupled;
    type Ingress = <input::Coupled as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for Gated
where
    W: wire::Spec,
    input::Coupled: input::Kind<W>,
{
    type Kind = input::Coupled;
    type Ingress = <input::Coupled as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for Scoped
where
    W: wire::Spec,
    input::Scoped: input::Kind<W>,
{
    type Kind = input::Scoped;
    type Ingress = <input::Scoped as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for GatedScoped
where
    W: wire::Spec,
    input::Scoped: input::Kind<W>,
{
    type Kind = input::Scoped;
    type Ingress = <input::Scoped as input::Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W> input::Mode<W> for Owned
where
    W: input::Execution,
{
    type Kind = input::Retained;
    type Ingress = W::Retained;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

impl<W, I> Mode<W> for I
where
    W: wire::Spec,
    I: Batch + input::Mode<W>,
{
    type Value<'input, 'w, 'd>
        = batch::Items<'input, W>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;
}

impl<W> Mode<W> for Scoped
where
    W: wire::Spec,
    Scoped: input::Mode<W>,
    <Scoped as input::Mode<W>>::Kind: input::Replay<W>,
{
    type Value<'input, 'w, 'd>
        = Chunk<'w, 'd, W, Disabled>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;
}

impl<W> Mode<W> for GatedScoped
where
    W: wire::Spec,
    GatedScoped: input::Mode<W>,
    <GatedScoped as input::Mode<W>>::Kind: input::Replay<W>,
{
    type Value<'input, 'w, 'd>
        = Chunk<'w, 'd, W, Disabled>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;
}

impl<W> Mode<W> for Owned
where
    W: wire::Spec,
    Owned: input::Mode<W>,
{
    type Value<'input, 'w, 'd>
        = Chunk<'w, 'd, W>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;
}
