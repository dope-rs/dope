/// Require both A and AAAA queries to avoid transport and server failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequireAll;
