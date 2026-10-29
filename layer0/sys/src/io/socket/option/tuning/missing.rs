/// A slot without a live pinned option chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Missing {
    Slot,
    Vacant,
}
