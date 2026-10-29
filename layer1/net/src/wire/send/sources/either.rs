use crate::wire::send::{self, Payload};

/// Explicit sum for policies which genuinely return either representation.
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<'a, A: Payload<'a>, B: Payload<'a>> Payload<'a> for Either<A, B> {
    fn select(&self) -> Option<impl send::raw::Ready<'a>> {
        match self {
            Self::Left(data) => data.select().map(Either::Left),
            Self::Right(data) => data.select().map(Either::Right),
        }
    }
}
