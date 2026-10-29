use dope_core::driver::route::Identity;
use dope_manifold::network::listener::conn;
use dope_net::wire::{
    self,
    receive::{self, discard, mode},
};

fn discard<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C>(
    conn: &mut conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C>,
) -> discard::Start
where
    W: wire::Spec + 'w + 'd,
    W::Receive: receive::Policy<W, Discard = mode::Enabled>,
{
    conn.discard(discard::Prefix::ONE)
}

fn main() {}
