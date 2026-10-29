use dope_runtime::{client::Provider, topology::lending};
fn recombine<'issued, 'life: 'issued, 'd: 'life, O, Q>(
    client: <O::Provider as Provider<'d>>::Client<'issued>,
    permit: lending::Permit<'issued, 'life, 'd, O, Q>,
) -> lending::Issued<'issued, 'life, 'd, O, Q>
where
    O: lending::Root<'d> + lending::raw::Resume<'d>,
    O::Provider: 'issued,
{
    lending::Issued::new(client, permit)
}

fn main() {}
