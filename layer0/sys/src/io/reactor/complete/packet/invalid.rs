/// The first invalid completion field, preserving its exact buffer failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Invalid<E> {
    Buffer(E),
    Flags,
}
