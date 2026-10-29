use std::num::NonZero;

/// The native backend does not support TCP fast-open listeners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Error(pub NonZero<i32>);
