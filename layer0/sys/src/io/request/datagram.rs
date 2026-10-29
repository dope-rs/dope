/// A lowered descriptor that retains the typed datagram submission proof.
pub trait Datagram {
    type Target;

    fn into_target(self) -> Self::Target;
}
