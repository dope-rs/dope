use o3::alloc;

use crate::ring::{create, layout::Layout, map::Map};

/// Exact startup failure and every owner whose cleanup remains incomplete.
pub enum Failure<S, M, P, C, R> {
    Allocation(alloc::Error),
    Setup(S),
    Layout(Layout),
    Mapping(Map<M>),
    Primary(P),
    Cleanup(C),
    Release(R),
}

impl<S, M, P, C, R> From<Result<P, C>> for Failure<S, M, P, C, R> {
    fn from(result: Result<P, C>) -> Self {
        match result {
            Ok(primary) => Self::Primary(primary),
            Err(cleanup) => Self::Cleanup(cleanup),
        }
    }
}

impl<S, M, P, C, R, A> From<create::Error<S, M, A>> for Failure<S, M, P, C, R>
where
    C: From<A>,
{
    fn from(error: create::Error<S, M, A>) -> Self {
        match error {
            create::Error::Setup(cause) => Self::Setup(cause),
            create::Error::Layout(layout) => Self::Layout(layout),
            create::Error::Map(cause) => Self::Mapping(cause),
            create::Error::Cleanup(state) => Self::Cleanup(C::from(state)),
        }
    }
}
