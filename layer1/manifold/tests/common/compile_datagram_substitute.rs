use dope_core::driver::route::KeyTag;
use dope_manifold::network::datagram::View;

fn substitute<'s, 'a: 's, 'b: 's>(
    view: View<'s, 'a, KeyTag<0>, (), ()>,
) -> View<'s, 'b, KeyTag<0>, (), ()>
where
    'a: 'b,
{
    view
}

fn main() {}
