use dope_net::link::egress::load::lease;
fn substitute<R: lease::Commit>(owner: R, body: R::Item) -> lease::Lease<R> {
    lease::Lease::new(owner, body)
}

fn main() {}
