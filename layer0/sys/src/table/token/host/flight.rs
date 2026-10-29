/// Consumes the affine completion authority at its exact terminal state.
pub trait Flight<F> {
    type Key;

    fn finish(self);
    fn end(self, flow: F) -> Self::Key;
}
