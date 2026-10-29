use std::{marker, mem};

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::wire;

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

/// Zero-sized projection of a session admission policy into its application.
pub struct Policy<M>(marker::PhantomData<fn(M) -> M>);

impl<M: admit::Mode> admit::Mode for Policy<M> {}

impl<'w, 'd: 'w, ID: Identity, N, W, M> admit::Dispatch<'d, ID, State<'w, 'd, ID, N, W>>
    for Policy<M>
where
    N: session::Parser<'d, ID> + Register<'d, ID, M>,
    W: wire::Spec + 'd,
    M: admit::Mode,
    State<'w, 'd, ID, N, W>: app::Application<'d, ID, Wire = W>,
{
    fn admit(
        state: &mut State<'w, 'd, ID, N, W>,
        active: &conn::Activation<'_, 'd, ID>,
    ) -> app::Admission {
        <N as Register<'d, ID, M>>::register(&state.session, active)
    }
}

const _: () = {
    assert!(mem::size_of::<Policy<admit::Automatic>>() == 0);
    assert!(mem::align_of::<Policy<admit::Automatic>>() == 1);
    assert!(!mem::needs_drop::<Policy<admit::Automatic>>());
};
