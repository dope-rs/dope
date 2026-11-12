use std::num::NonZero;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event<'a> {
    /// The entry name, including its terminating NUL and native zero padding.
    MovedIn(&'a [u8]),
    Overflow,
    Invalidated(NonZero<u32>),
}
