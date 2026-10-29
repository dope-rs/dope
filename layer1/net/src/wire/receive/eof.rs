use dope_core::driver::route::Identity;

use crate::wire::{self, receive, terminal};

/// EOF completes without inspecting wire state.
pub enum Clean {}

/// EOF is classified from the exact wire connection state.
pub enum State {}

/// Stateful EOF classification owned by a receive policy.
pub trait Eof<W: wire::Spec>: receive::Policy<W, Eof = State> {
    fn eof<'w, ID: Identity + 'w>(
        wire: &mut W::Connection<'w, ID>,
        end: terminal::Send<'_, terminal::Error<'w, W>>,
    ) -> terminal::Processed<()>
    where
        W: 'w;
}
