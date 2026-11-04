use crate::reactor::{Held, buffers::Canary, register, ring, ring::session::GROUP};

pub(crate) type Closing<T, L, F> =
    Held<register::raw::Closing<Canary<L>, GROUP, ring::Closing<T>>, F>;
