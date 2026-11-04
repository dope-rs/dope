use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::pool::recv::input;

use crate::client::{
    app::{self, admit},
    conn,
    session::{self, Session, app::State},
};

/// Admission required by the session's selected connection mode.
pub trait Register<'d, ID: Identity = KeyTag<0>, M: admit::Mode = admit::Registered> {
    fn register(&self, active: &conn::Activation<'_, 'd, ID>) -> app::Admission;
}

impl<'d, ID: Identity, N> Register<'d, ID, admit::Automatic> for N
where
    N: Session<'d, ID>,
{
    fn register(&self, _: &conn::Activation<'_, 'd, ID>) -> app::Admission {
        app::Admission::Attached
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> admit::Register<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Connected<'d, ID>,
    W: input::Execution + 'd,
{
    fn register(&mut self, active: &conn::Activation<'_, 'd, ID>) -> app::Admission {
        <N as Register<'d, ID, N::Activation>>::register(&self.session, active)
    }
}
