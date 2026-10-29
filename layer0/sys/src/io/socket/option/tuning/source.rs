use crate::io::socket::option::tuning::{self, Table};

/// Selects native owners for pinned option values and their target echoes.
pub trait Source {
    type State<E: Copy, const N: usize>: tuning::raw::State<E, N>;
    type Table<E: Copy, const N: usize>: Table<E, N, State = Self::State<E, N>>;
}
