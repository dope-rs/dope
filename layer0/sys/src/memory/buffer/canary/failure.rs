/// Rejected selection evidence, or the caller's exact payload-check failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure<I, L, C, E> {
    Index(I),
    Length { actual: L, capacity: C },
    Inspect(E),
}
