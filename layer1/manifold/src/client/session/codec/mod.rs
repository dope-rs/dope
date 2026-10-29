use std::num;

pub use bytes::Bytes;
use dope_core::io::transfer;
pub use input::Input;
use o3::{
    buffer,
    buffer::{hold, resident, write::consume::scope},
};

use crate::client::session::response;

mod bytes;
pub mod eof;
mod input;
pub mod mode;
pub mod staging;
pub mod storage;

/// Initialized storage and state borrowed for one stateful codec operation.
pub type Owner<'slot, 'id, 'd, C, S> =
    storage::Owner<'slot, 'id, <C as Stateful>::Storage<S>, <C as Stateful>::State<'d>>;

/// Proof that an incremental parser's progress and next reservation are valid.
pub struct Need<'id> {
    consumed: Option<scope::End<'id, { transfer::MAX_BYTES as u32 }>>,
    target: Option<Exact>,
}

/// Incremental parse result with explicit progress and capacity states.
pub enum Parse<'input, T> {
    More(Need<'input>),
    Blocked,
    CapacityExhausted,
    Item {
        head: T,
        consumed: scope::End<'input, { transfer::MAX_BYTES as u32 }>,
    },
}

#[must_use]
pub enum Finish<T> {
    Empty,
    CapacityExhausted,
    Item(T),
}

/// A scoped codec completion returns its original owner with the delivery outcome.
pub type Finished<O, T> = (O, Result<Finish<T>, T>);

/// The original stateless codec's EOF head and exact failure.
pub type End<'input, 'd, C, S> =
    Result<Finish<<C as Codec>::Head<'input, 'd, S>>, <C as Codec>::Error>;

/// Admission while input and semantic parser state remain replayable.
/// `Ready` accepts ownership; exact later failures cannot request input replay.
/// Owned post-commit work uses the existing prepared delivery.
#[must_use]
pub enum Admission<F> {
    RetryInput,
    Ready(F),
}

/// One decode invocation's progress, paired with its committed input end.
pub struct Step<T>(pub(crate) Progress<T>);

/// Internal progress built by `Input` from a codec's `Need` and returned as `Step`.
/// `More` retains the crate-sealed `Exact` reservation count.
pub(crate) enum Progress<T> {
    More {
        advanced: bool,
        target: Option<Exact>,
    },
    Blocked,
    CapacityExhausted,
    RetryInput,
    Rejected(T),
    Item(T),
}

impl<T> Step<T> {
    pub const fn blocked() -> Self {
        Self(Progress::Blocked)
    }

    pub const fn rejected(delivery: T) -> Self {
        Self(Progress::Rejected(delivery))
    }

    /// Whether this step committed the input end it observed.
    pub const fn has_consumed(&self) -> bool {
        match self.0 {
            Progress::More { advanced, .. } => advanced,
            Progress::Item(_) => true,
            Progress::Blocked
            | Progress::CapacityExhausted
            | Progress::RetryInput
            | Progress::Rejected(_) => false,
        }
    }
}

const _: () = assert!(
    size_of::<Option<scope::End<'static, { transfer::MAX_BYTES as u32 }>>>()
        == size_of::<transfer::NonZero>()
);

pub trait Codec {
    /// A decoded item borrowing `input`; retained storage is branded by `d`.
    /// Bytes kept beyond a response callback must be retained into `d`.
    type Head<'input, 'd, R: buffer::Cursor>
    where
        R: 'input,
        'd: 'input;
    type Error;
    type Staging: staging::Policy;

    /// Statically selected ingress policy and its validated runtime plan.
    fn staging(&self) -> <Self::Staging as staging::Policy>::Spec;
}

/// Decodes frames without connection-local parser state.
pub trait Stateless: Codec {
    fn parse<'id, 'input, 'd, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input;
}

/// Decodes frames with connection-local parser state branded by `d`.
pub trait Stateful: Codec {
    type State<'d>;
    type Storage<S>;

    /// A loan into the current decode plan, valid only during admission.
    type Preview<'a>;

    /// An owned value or index retained into `d` beyond its decode input.
    /// Omitting the input lifetime avoids higher-ranked or `'static` output bounds.
    type Outcome<'d>;

    fn state<'d>(&self, budget: resident::Budget<'d>) -> Self::State<'d>;

    fn storage<'d, S>(&self, source: S, state: &Self::State<'d>) -> Self::Storage<S>;

    fn source<S: buffer::raw::Source>(storage: &Self::Storage<S>) -> &S;

    /// Lends the exact source while scanner execution is suspended.
    /// Ingress preserves its prefix or restarts metadata before decoding resumes.
    fn edit<S>(storage: &mut Self::Storage<S>) -> storage::Edit<'_, S>;

    /// Restarts scanner metadata from the current controls, retaining the same source.
    /// The returned loan permits destructive edits before decoding resumes.
    fn restart<'a, 'd, S>(
        &self,
        storage: &'a mut Self::Storage<S>,
        state: &Self::State<'d>,
    ) -> storage::Edit<'a, S>;

    fn decode<'d, S, T, F>(
        &self,
        storage: &mut Self::Storage<S>,
        state: &mut Self::State<'d>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> Admission<F>,
    ) -> Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut Self::State<'d>) -> T::Accepted;

    fn finish<'slot, 'id, 'd, S, T, F>(
        &self,
        input: storage::Owner<'slot, 'id, Self::Storage<S>, Self::State<'d>>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> Admission<F>,
    ) -> Finished<storage::Owner<'slot, 'id, Self::Storage<S>, Self::State<'d>>, T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut Self::State<'d>) -> T::Accepted;
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Exact(num::NonZero<u32>);

impl Exact {
    const fn from_valid(target: usize) -> Self {
        Self(num::NonZero::<u32>::MIN.saturating_add(target as u32 - 1))
    }

    pub(in crate::client::session) const fn count(self) -> num::NonZero<u32> {
        self.0
    }
}
