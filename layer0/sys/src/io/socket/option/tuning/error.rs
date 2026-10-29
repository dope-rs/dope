use crate::io::reactor::Full;

/// A rejected option chain retains its exact descriptor owner.
#[derive(Debug)]
pub enum Error<F, E> {
    Full(F, Full),
    Reserve(F, E),
}
