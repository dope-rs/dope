use dope_net::link::egress::{NonEmpty, load::lease};
fn detach<R: lease::Commit>(value: NonEmpty<lease::Lease<R>>) -> (R, R::Item) {
    value.into_inner().into_parts()
}

fn main() {}
