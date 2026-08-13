use std::convert;

use crate::driver::storage;

pub trait Never: storage::Impossible {
    fn never<T>(self) -> T;
}

impl Never for convert::Infallible {
    fn never<T>(self) -> T {
        match self {}
    }
}

impl<A: Never, B: Never> Never for storage::PairError<A, B> {
    fn never<T>(self) -> T {
        match self {
            Self::First(error) => error.never(),
            Self::Second(error) => error.never(),
        }
    }
}
