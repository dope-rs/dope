use crate::ring::{layout::Layout, map::Map};

/// The original ring failure, independent of subsequent cleanup failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause<S, M> {
    Setup(S),
    Layout(Layout),
    Map(Map<M>),
}
