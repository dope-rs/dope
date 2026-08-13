use std::convert;

use crate::driver::storage;

pub trait Impossible {}

impl Impossible for convert::Infallible {}

impl<A: Impossible, B: Impossible> Impossible for storage::PairError<A, B> {}
