/// A fixed-file slot remains occupied until removal or ring teardown succeeds.
pub trait Occupied {
    type Target;

    fn slot(&self) -> Self::Target;
}
