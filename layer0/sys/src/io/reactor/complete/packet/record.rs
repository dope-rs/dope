use crate::io::reactor::complete;

/// Preserves native fields while mapping the affine selected-buffer owner.
pub trait Map: complete::Map {
    type Parts: Copy;
    type Fault;

    fn new<B>(parts: Self::Parts, buffer: Selection<B, Self::Fault>) -> Self::Entry<B>;
    fn into_parts<B>(entry: Self::Entry<B>) -> (Self::Parts, Selection<B, Self::Fault>);
}

/// A selected owner or the exact fault that prevented selecting it.
pub enum Selection<B, E> {
    None,
    Selected(B),
    Invalid(E),
}
