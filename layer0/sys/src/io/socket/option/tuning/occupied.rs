/// One pinned option chain awaiting its unique terminal completion.
pub trait Occupied<E> {
    fn target(&self) -> E;
    fn clear(self);
}
