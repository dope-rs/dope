use crate::{
    engine::admit::{Context, Error, Native, Submission},
    io::request,
};

type Failure<C> = Error<<<C as Context>::Native as Native>::Error<<C as Context>::Failure>>;

/// Binds the exact host key and pending state before reserving native capacity.
///
/// Descriptor-local admission also retains the slot's matching operation when
/// its native requests use a descriptor-local token.
pub trait Reserve<'a, C: Context, B: 'a>: Submission {
    fn try_new(backend: &'a mut B, key: C::Key, pending: bool) -> Result<Self, Failure<C>>;

    /// Binds a descriptor-local cancellation to its exact flight identity.
    /// The caller retains its descriptor and target on every admission outcome.
    fn try_at(
        backend: &'a mut B,
        key: C::Key,
        pending: bool,
        _slot: &<C::Inputs as request::Context>::Target,
    ) -> Result<Self, Failure<C>> {
        Self::try_new(backend, key, pending)
    }
}
