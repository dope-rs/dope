use crate::table::token::{Codec, host};

/// Selects the native kernel identity codec for one host context.
pub trait Source {
    type Token<C: host::Context>: Codec<Context = C>;
}
