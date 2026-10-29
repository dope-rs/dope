pub mod fd;
pub mod file;
pub mod pipe;
pub mod poll;
pub mod socket;
use std::convert;

use o3::mem::Rejected;

pub mod raw;
pub mod reactor;
pub mod request;

/// Consumes an owner, retaining failed cleanup for retry.
pub trait Close: Sized {
    type State: Retry;

    fn try_close(self) -> Result<(), Self::State>;
}

/// Returns the exact failure and its live owner before another cleanup attempt.
pub trait Retry: Sized {
    type Failure;
    type Owner: Close<State = Self>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure>;
}

/// Exact failure from either phase of an owner followed by its retained storage.
pub enum Cleanup<O, S> {
    Owner(O),
    Storage(S),
}

impl Retry for convert::Infallible {
    type Failure = Self;
    type Owner = Self;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        match self {}
    }
}

impl Close for convert::Infallible {
    type State = Self;

    fn try_close(self) -> Result<(), Self::State> {
        match self {}
    }
}

impl<O, E> Retry for Rejected<O, E>
where
    O: Close<State = Self>,
{
    type Failure = E;
    type Owner = O;

    fn recover(self) -> Rejected<O, E> {
        self
    }
}
