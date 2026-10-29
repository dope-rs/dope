use dope_net::link::egress::{NonEmpty, load::lease};
fn escape<'a, R: lease::Commit>(value: NonEmpty<lease::Lease<R>>) -> impl Sized + 'a {
    value
}

fn main() {}
