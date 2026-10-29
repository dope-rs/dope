/// EOF reached while a pending codec still owns partial input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Truncated;

/// An outbound-only protocol received peer bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Inbound;
