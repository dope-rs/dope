use crate::table::token::host::Cancel;

/// Native identity classification after exact host ownership binding.
pub enum Bound<E, C, F, B, K, T> {
    Close(E),
    CloseClass { echo: E, expected: C, observed: C },
    Framework(F),
    Flight(B),
    Cancel(Cancel<K, T>),
    Invalid,
}
