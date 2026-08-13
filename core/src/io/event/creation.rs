use std::mem;

use crate::{
    driver::route,
    io::{self, event},
};

/// A socket-creation completion and its affine driver target.
///
/// Safe code cannot mint completion authority.
///
/// ```compile_fail
/// use dope_core::{driver::route, io::{self, event::creation}};
///
/// let _ = creation::Completion::new(
///     route::SHUTDOWN,
///     io::SocketEvent::Failed(std::io::Error::other("not a driver completion")),
/// );
/// ```
///
/// Completion authority is consumed exactly once.
///
/// ```compile_fail
/// use dope_core::io::event::creation;
///
/// fn consume(_: creation::Completion<'static>) {}
/// fn replay(completion: creation::Completion<'static>) {
///     consume(completion);
///     consume(completion);
/// }
/// ```
#[repr(transparent)]
pub struct Completion<'d> {
    targeted: event::Targeted<io::SocketEvent<'d>>,
}

const _: () = {
    assert!(
        mem::size_of::<Completion<'static>>()
            == mem::size_of::<(route::Token, io::SocketEvent<'static>)>()
    );
    assert!(
        mem::align_of::<Completion<'static>>()
            == mem::align_of::<(route::Token, io::SocketEvent<'static>)>()
    );
};

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(token: route::Token, event: io::SocketEvent<'d>) -> Self {
        Self {
            targeted: event::Targeted::new(token, event),
        }
    }

    pub const fn token(&self) -> route::Token {
        self.targeted.token()
    }

    pub const fn event(&self) -> &io::SocketEvent<'d> {
        self.targeted.value()
    }

    pub fn into_parts(self) -> (route::Token, io::SocketEvent<'d>) {
        self.targeted.into_parts()
    }
}
