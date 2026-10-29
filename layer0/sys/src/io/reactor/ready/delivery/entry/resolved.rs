#[must_use = "a completion must be delivered, reclaimed, or rejected"]
pub enum Resolved<C, E, F> {
    Public(C),
    Reclaim(E),
    Rejected(F),
}
