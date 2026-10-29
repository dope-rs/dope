use std::mem;

use dope_sys::{Errno, kernel};

use crate::reactor::cq::{Invalid, Outcome, Value};

/// The exact completion result rejected by its operation's contract.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Returned(Outcome);

impl From<Outcome> for Returned {
    fn from(outcome: Outcome) -> Self {
        Self(outcome)
    }
}

impl kernel::Returned for Returned {
    type Value = Value;
    type Invalid = Invalid;

    fn value(value: Value) -> Self {
        Self(value.into_outcome())
    }

    fn error(code: Errno) -> Self {
        Self(Outcome::from_error(code))
    }

    fn invalid(invalid: Invalid) -> Self {
        Self(invalid.into_outcome())
    }
}

const _: () = {
    assert!(mem::size_of::<Returned>() == mem::size_of::<Outcome>());
    assert!(mem::align_of::<Returned>() == mem::align_of::<Outcome>());
    assert!(mem::size_of::<Option<Returned>>() == mem::size_of::<Option<Outcome>>());
};
