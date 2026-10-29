use dope_net::link::egress::metadata::arena;
use o3::{alloc, base::cell::region};

fn duplicate<'p, 'd>(
    token: &region::Token<region::Identity<'d>>,
    store: &'p mut arena::Store<'d>,
) -> Result<
    (
        arena::Arena<'p, 'd, &'d [u8]>,
        arena::Arena<'p, 'd, &'d [u8]>,
    ),
    alloc::Error,
> {
    let first = arena::Arena::try_new(token, store)?;
    let second = arena::Arena::try_new(token, store)?;
    Ok((first, second))
}

fn main() {}
