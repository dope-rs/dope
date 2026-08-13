use std::mem;

use crate::{
    driver::route,
    io::{self, event},
};

#[repr(transparent)]
pub struct Completion {
    targeted: event::Targeted<io::SendEvent>,
}

const _: () = {
    assert!(mem::size_of::<Completion>() == mem::size_of::<(route::Token, io::SendEvent)>());
    assert!(mem::align_of::<Completion>() == mem::align_of::<(route::Token, io::SendEvent)>());
};

impl Completion {
    pub(in crate::io) const fn new(token: route::Token, event: io::SendEvent) -> Self {
        Self {
            targeted: event::Targeted::new(token, event),
        }
    }

    pub const fn token(&self) -> route::Token {
        self.targeted.token()
    }

    pub const fn event(&self) -> io::SendEvent {
        *self.targeted.value()
    }

    pub const fn into_parts(self) -> (route::Token, io::SendEvent) {
        self.targeted.into_copy_parts()
    }
}
