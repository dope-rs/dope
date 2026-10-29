/// Exact completion evidence rejected by a native capability canary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Check<O> {
    InvalidFlags(O),
    Rejected(O),
    Unexpected(O),
    Nonterminal(O),
}
