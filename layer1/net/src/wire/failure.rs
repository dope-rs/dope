use std::convert::Infallible;

/// Open-error family selected by a wire's failure class.
pub enum Open {}

/// Terminal-error family selected by a wire's failure class.
pub enum Terminal {}

/// Compile-time selection of an exact wire-failure family.
pub trait Class<W: ?Sized, K> {
    type Error<'d>: 'd
    where
        W: 'd;
}

/// Exact driver-scoped error family for one wire transition.
pub trait Errors<K> {
    type Error<'d>: 'd
    where
        Self: 'd;
}

/// The transition can fail and its final consumer must accept the exact cause.
pub struct Possible;

/// The transition's failure is uninhabited, so it needs no consumer callback.
pub struct Never;

impl<W: Errors<K>, K> Class<W, K> for Possible {
    type Error<'d>
        = W::Error<'d>
    where
        W: 'd;
}

impl<W: ?Sized, K> Class<W, K> for Never {
    type Error<'d>
        = Infallible
    where
        W: 'd;
}
