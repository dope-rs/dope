/// One pinned option chain awaiting its unique terminal completion.
pub trait Occupied<E> {
    fn target(&self) -> E;
    /// The terminal failure came from a submitted scalar option.
    fn option_failed(&self) -> bool;
    /// Expected native result size and value; inspect only after the terminal.
    fn output(&self) -> (usize, u32);
    fn clear(self);
}
