use std::{io, mem};

use crate::{driver::route, io::event};

pub enum Outcome {
    Connected,
    Failed(io::Error),
}

/// A connection completion and its affine driver target.
///
/// Safe code cannot mint completion authority.
///
/// ```compile_fail
/// use dope_core::{driver::route, io::event::connect};
///
/// let _ = connect::Completion::new(route::SHUTDOWN, connect::Outcome::Connected);
/// ```
///
/// Completion authority is consumed exactly once.
///
/// ```compile_fail
/// use dope_core::io::event::connect;
///
/// fn consume(_: connect::Completion) {}
/// fn replay(completion: connect::Completion) {
///     consume(completion);
///     consume(completion);
/// }
/// ```
#[repr(transparent)]
pub struct Completion {
    targeted: event::Targeted<Outcome>,
}

const _: () = {
    assert!(mem::size_of::<Completion>() == mem::size_of::<(route::Token, Outcome)>());
    assert!(mem::align_of::<Completion>() == mem::align_of::<(route::Token, Outcome)>());
};

impl Completion {
    pub(in crate::io) const fn new(token: route::Token, outcome: Outcome) -> Self {
        Self {
            targeted: event::Targeted::new(token, outcome),
        }
    }

    pub const fn token(&self) -> route::Token {
        self.targeted.token()
    }

    pub const fn outcome(&self) -> &Outcome {
        self.targeted.value()
    }

    pub fn into_parts(self) -> (route::Token, Outcome) {
        self.targeted.into_parts()
    }
}
