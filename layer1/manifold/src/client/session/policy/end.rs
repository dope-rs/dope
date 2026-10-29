use dope_core::driver::route::{Identity, KeyTag};
use dope_net::wire::{self, failure, terminal};
use o3::base::cell::region;

use crate::client::conn;

/// Final typed sink for one established wire's terminal failure.
pub trait Sink<
    'w,
    'd,
    W: wire::Spec<TerminalFailure = M> + 'w,
    ID: Identity = KeyTag<0>,
    M = <W as wire::Spec>::TerminalFailure,
>
{
    fn fail(
        &self,
        conn: conn::Id<'d, ID>,
        cause: terminal::Error<'w, W>,
        region: &mut region::Token<region::Identity<'d>>,
    );
}

impl<'w, 'd, W, P: ?Sized, ID: Identity> Sink<'w, 'd, W, ID, failure::Never> for P
where
    W: wire::Spec<TerminalFailure = failure::Never> + 'w,
{
    fn fail(
        &self,
        _: conn::Id<'d, ID>,
        cause: terminal::Error<'w, W>,
        _: &mut region::Token<region::Identity<'d>>,
    ) {
        cause.eliminate()
    }
}
