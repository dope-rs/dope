use crate::table::token::host;

/// Affine close encoding retaining the descriptor domain lifetime.
pub trait Close<'d>: Sized {
    type Context: host::Context;

    fn new(close: &<Self::Context as host::Context>::Close<'d>) -> Self;
    fn retire(retire: &<Self::Context as host::Context>::Retire<'d>) -> Self;
    fn raw(self) -> u64;
    fn decode(raw: u64) -> Option<<Self::Context as host::Context>::Returned>;
}
