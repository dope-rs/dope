use std::{io, mem};

use crate::{driver::route, io::event};

pub enum Outcome {
    Applied,
    Failed(io::Error),
}

/// A terminal socket-option transaction completion.
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
