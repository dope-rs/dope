use dope_core::driver::route::{Identity, KeyTag};
use dope_net::wire;

use crate::client::{app::request, session};

/// Produces application requests for one exact connection generation.
///
/// Sessions without an outbound request source do not implement this capability.
pub trait Source<'d, ID: Identity = KeyTag<0>>:
    session::Parser<'d, ID> + session::Session<'d, ID, Requests = request::Enabled>
{
    fn drain<'w, 'e, 'turn, W: wire::Spec + 'd>(
        &self,
        turn: &mut request::Turn<
            'turn,
            'e,
            'd,
            session::Connection<'w, 'd, ID, Self, W>,
            Self::Send,
            ID,
        >,
    ) where
        'd: 'w;
}
