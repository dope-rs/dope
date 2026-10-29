use dope_net::link::egress::data::pair;
use o3::buffer::{Residency, pool};
fn forge<C: AsRef<[u8]> + Residency>(
    prefix: pool::Cursor,
    body: C,
) -> Result<pair::Source<C>, pair::Rejected<C>> {
    let value = pair::Source::try_new(prefix, body)?;
    Ok(value.into_inner())
}

fn main() {}
