use crate::probe::Failure;

/// Native completion checks retain the caller's identity and exact observation.
pub trait Outcome: Sized {
    type Parts;
    type Value;
    type Meta: Copy;
    type Probe: Copy;
    type Observation;

    fn new(
        parts: Self::Parts,
        probe: Self::Probe,
    ) -> Result<Self, Failure<Self::Probe, Self::Observation>>;
    fn success(&self) -> Result<Self::Value, Failure<Self::Probe, Self::Observation>>;
    fn zero(self) -> Result<(), Failure<Self::Probe, Self::Observation>>;
    fn canceled(self) -> Result<(), Failure<Self::Probe, Self::Observation>>;
    fn cancel(self) -> Result<(), Failure<Self::Probe, Self::Observation>>;
    fn probe(&self) -> Self::Probe;
    fn meta(&self) -> Self::Meta;
    fn unexpected(&self) -> Failure<Self::Probe, Self::Observation>;
    fn nonterminal(&self) -> Failure<Self::Probe, Self::Observation>;
}
