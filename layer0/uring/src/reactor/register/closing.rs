use crate::reactor::{Held, register, ring};

pub(crate) type Closing<T> = Held<ring::Closing<T>, register::raw::Occupied>;
