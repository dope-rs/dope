use std::pin;

use dope_core::driver::route::Identity;

use crate::network::listener::{conn, handler};

/// Type-level selection of a final listener owner audit.
pub trait Policy<'d, A: handler::Application<'d, ID>, ID: Identity>: Sized {
    fn select() -> Option<Self>;

    fn retire(
        self,
        app: pin::Pin<&mut A>,
        conn: conn::Retired<'_, '_, 'd, ID, A::Wire, A::Conn, A::Egress>,
    );
}

/// Releases retired state without an application callback.
pub enum Close {}

impl<'d, A, ID: Identity> Policy<'d, A, ID> for Close
where
    A: handler::Application<'d, ID>,
{
    fn select() -> Option<Self> {
        None
    }

    fn retire(
        self,
        _: pin::Pin<&mut A>,
        _: conn::Retired<'_, '_, 'd, ID, A::Wire, A::Conn, A::Egress>,
    ) {
        match self {}
    }
}
