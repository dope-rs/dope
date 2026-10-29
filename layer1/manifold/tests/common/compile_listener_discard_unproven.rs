use dope_core::driver::route::Identity;
use dope_manifold::network::listener::conn;
use dope_net::wire::{self, receive::discard};

fn discard<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C>(
    conn: &mut conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C>,
) -> discard::Start
where
    W: wire::Spec + 'w + 'd,
{
    conn.discard(discard::Prefix::ONE)
}

fn main() {}
