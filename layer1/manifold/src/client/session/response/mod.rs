use std::{convert::Infallible, marker, mem};

use dope_core::driver::route::Identity;

use crate::client::session::{self, turns};

mod action;
mod effect;
mod sealed;
pub use action::Action;

pub mod flow;
pub mod work;

pub(crate) use effect::Effect;
pub(super) use sealed::Sealed;

type CommitState<P> = marker::PhantomData<(P, fn(P) -> P)>;
type ResumeState<P, Q> = marker::PhantomData<(P, Q, fn(P, Q) -> (P, Q))>;

/// Result of delivering a response that owns no post-commit work.
#[derive(Clone, Copy, Eq, PartialEq)]
#[must_use]
pub enum Direct {
    Committed,
    RetryInput,
}

/// Selects direct response delivery with no post-commit work.
pub struct Immediate;

/// Selects one consuming post-commit callback with no continuation.
pub struct Commit<P>(CommitState<P>);

/// Selects resumable post-commit work.
pub struct Resume<P, Q>(ResumeState<P, Q>);

/// Linear proof that input owning one preparation has committed or been discarded.
#[repr(transparent)]
#[must_use]
pub struct Committed<P>(P);

/// Connection-owned work that can exist only after input commitment.
///
/// ```compile_fail
/// use dope_manifold::client::session::response::Item;
/// fn before_commit<P>(prepared: P) -> Item<P> { Item::new(prepared) }
/// ```
#[repr(transparent)]
#[must_use]
pub struct Item<P>(P);

impl<P> Committed<P> {
    pub(crate) const fn new(item: P) -> Self {
        Self(item)
    }

    pub fn into_inner(self) -> P {
        self.0
    }

    pub fn into_item(self) -> Item<P> {
        Item(self.0)
    }
}

impl<P> Item<P> {
    pub(crate) const fn new(item: P) -> Self {
        Self(item)
    }

    pub fn into_inner(self) -> P {
        self.0
    }
}

/// Causal result of preparing one decoded response.
#[must_use]
pub enum Delivery<P> {
    Committed,
    RetryInput,
    Prepared(P),
}

impl Sealed for Direct {}
impl<P> Sealed for Delivery<P> {}

/// The concrete delivery carriers preserve accepted ownership structurally.
pub trait Outcome: Sized + Sealed {
    /// A delivery that cannot reject already accepted decode ownership.
    type Accepted;

    fn accept(accepted: Self::Accepted) -> Self;
    fn retry_input() -> Self;
    fn committed() -> Self;
    fn disposition(&self) -> Direct;
}

impl Outcome for Direct {
    type Accepted = ();

    fn accept((): ()) -> Self {
        Self::Committed
    }

    fn retry_input() -> Self {
        Self::RetryInput
    }

    fn committed() -> Self {
        Self::Committed
    }

    fn disposition(&self) -> Direct {
        *self
    }
}

impl<P> Outcome for Delivery<P> {
    type Accepted = Option<P>;

    fn accept(accepted: Option<P>) -> Self {
        match accepted {
            Some(prepared) => Self::Prepared(prepared),
            None => Self::Committed,
        }
    }

    fn retry_input() -> Self {
        Self::RetryInput
    }

    fn committed() -> Self {
        Self::Committed
    }

    fn disposition(&self) -> Direct {
        match self {
            Self::RetryInput => Direct::RetryInput,
            Self::Committed | Self::Prepared(_) => Direct::Committed,
        }
    }
}

/// Result of consuming one linear response continuation.
#[must_use]
pub enum Settlement<P> {
    Ready,
    Yield(P),
    Blocked(P),
}

/// Owns the exact response values and connection-owned flow state.
pub trait Plan {
    type Delivery: Outcome;
    type Flow: flow::Policy;
    type Prepared;
    type Pending;
}

impl Plan for Immediate {
    type Delivery = Direct;
    type Flow = flow::State;
    type Prepared = Infallible;
    type Pending = Infallible;
}

impl<P> Plan for Commit<P> {
    type Delivery = Delivery<P>;
    type Flow = flow::State;
    type Prepared = P;
    type Pending = Infallible;
}

impl<P, Q> Plan for Resume<P, Q> {
    type Delivery = Delivery<P>;
    type Flow = flow::Pending<Item<Q>>;
    type Prepared = P;
    type Pending = Q;
}

/// Executes one response plan for its exact session proof.
pub trait Mode<'d, ID: Identity, N: session::Session<'d, ID>>: Plan {
    fn apply<'e, A: work::Admission>(
        state: &mut Self::Flow,
        owner: &mut N,
        delivery: Self::Delivery,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        eof: bool,
        end: flow::End,
    ) -> Effect;
}

impl<'d, ID: Identity, N: session::Session<'d, ID>> Mode<'d, ID, N> for Immediate {
    fn apply<'e, A: work::Admission>(
        _: &mut flow::State,
        _: &mut N,
        delivery: Direct,
        _: &mut A,
        _: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        _: bool,
        end: flow::End,
    ) -> Effect {
        match delivery {
            Direct::Committed => Effect::from_end(end),
            Direct::RetryInput => Effect::RetryInput,
        }
    }
}

impl<'d, ID: Identity, N, P> Mode<'d, ID, N> for Commit<P>
where
    N: session::Commit<'d, P, ID>,
{
    fn apply<'e, A: work::Admission>(
        _: &mut flow::State,
        owner: &mut N,
        delivery: Delivery<P>,
        _: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        _: bool,
        end: flow::End,
    ) -> Effect {
        let direct = match delivery {
            Delivery::Committed => Direct::Committed,
            Delivery::RetryInput => Direct::RetryInput,
            Delivery::Prepared(item) => {
                owner.commit(Committed::new(item), ctx);
                Direct::Committed
            }
        };
        match direct {
            Direct::Committed => Effect::from_end(end),
            Direct::RetryInput => Effect::RetryInput,
        }
    }
}

impl<'d, ID: Identity, N, P, Q> Mode<'d, ID, N> for Resume<P, Q>
where
    N: session::Resume<'d, P, Q, ID>,
{
    fn apply<'e, A: work::Admission>(
        state: &mut flow::Pending<Item<Q>>,
        owner: &mut N,
        delivery: Delivery<P>,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        eof: bool,
        end: flow::End,
    ) -> Effect {
        match delivery {
            Delivery::Committed => Effect::from_end(end),
            Delivery::RetryInput => Effect::RetryInput,
            Delivery::Prepared(item) => match owner.commit(Committed::new(item), work, ctx) {
                Settlement::Ready => Effect::from_end(end),
                Settlement::Yield(item) => {
                    state.ready(eof, end, Item::new(item));
                    Effect::Yield
                }
                Settlement::Blocked(item) => {
                    state.block(eof, end, Item::new(item));
                    Effect::Blocked
                }
            },
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Committed<usize>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Item<usize>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Committed<usize>>() == mem::align_of::<usize>());
    assert!(mem::align_of::<Item<usize>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Commit<usize>>() == 0);
    assert!(mem::size_of::<Resume<usize, u8>>() == 0);
};
